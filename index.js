const express = require('express');
const axios = require('axios');
const { HttpProxyAgent, HttpsProxyAgent } = require('hpagent');
const { EventEmitter } = require('events');
const fs = require('fs').promises;
const path = require('path');

class ProxyPool extends EventEmitter {
  constructor(options = {}) {
    super();
    this.proxies = new Map(); // Format: { proxy: { latency: number, lastChecked: Date, successRate: number, totalChecks: number } }
    this.blacklist = new Set(); // Permanently failed proxies
    this.lastFullUpdate = null;
    this.isValidating = false;
    this.validationQueue = [];
    this.priorityQueue = []; // High-priority validation queue
    this.stats = {
      totalFetched: 0,
      validCount: 0,
      blacklistedCount: 0,
      lastUpdateDuration: 0,
      successfulValidations: 0,
      failedValidations: 0
    };

    this.config = {
      sources: [
        'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text&timeout=20000',
        'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
        'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt',
        'https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt',
        'https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt',
        'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt',
        'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt'
      ],
      validationUrls: [
        'http://httpbin.org/ip',
        'https://api.ipify.org?format=json',
        'http://ip-api.com/json/?fields=query'
      ],
      checkTimeout: 8000,
      concurrency: 100,
      updateInterval: 60 * 60 * 1000, // 1 hour
      minUpdateInterval: 30 * 60 * 1000, // 30 minutes
      revalidationInterval: 30 * 60 * 1000, // 30 minutes
      maxBlacklistTime: 24 * 60 * 60 * 1000, // 24 hours
      minSuccessRate: 0.3, // 30% success rate to keep proxy
      maxRetries: 3,
      persistenceFile: './proxy-data.json',
      ...options
    };
  }

  async initialize() {
    try {
      // Load persisted data
      await this.loadPersistedData();
      
      // Always fetch proxies on first run
      console.log('Performing initial proxy fetch...');
      await this.fetchNewProxies();
      
      // Start periodic updates
      setInterval(() => this.fetchNewProxies(), this.config.updateInterval);
      
      // Start revalidation of existing proxies
      setInterval(() => this.revalidateExistingProxies(), this.config.revalidationInterval);
      
      // Start validation workers
      this.startValidationWorker();
      
      // Periodic cleanup and persistence
      setInterval(() => {
        this.cleanupBlacklist();
        this.persistData();
      }, 10 * 60 * 1000); // Every 10 minutes
      
      console.log(`ProxyPool initialized with ${this.proxies.size} cached proxies`);
    } catch (error) {
      console.error('Failed to initialize ProxyPool:', error.message);
      throw error;
    }
  }

  async loadPersistedData() {
    try {
      const data = await fs.readFile(this.config.persistenceFile, 'utf8');
      const parsed = JSON.parse(data);
      
      // Only restore blacklist, not proxies (always start fresh)
      parsed.blacklist?.forEach(proxy => this.blacklist.add(proxy));
      
      this.lastFullUpdate = parsed.lastFullUpdate ? new Date(parsed.lastFullUpdate) : null;
      console.log(`Loaded ${this.blacklist.size} blacklisted proxies from cache (proxies will be fetched fresh)`);
    } catch (error) {
      console.log('No cached data found, starting fresh');
    }
  }

  async persistData() {
    try {
      const data = {
        blacklist: Array.from(this.blacklist), // Only persist blacklist, not active proxies
        lastFullUpdate: this.lastFullUpdate,
        timestamp: new Date()
      };
      
      await fs.writeFile(this.config.persistenceFile, JSON.stringify(data, null, 2));
    } catch (error) {
      console.error('Failed to persist data:', error.message);
    }
  }

  async fetchNewProxies() {
    if (this.isValidating && this.validationQueue.length > 1000) {
      console.log('Large validation queue, skipping new fetch');
      return;
    }

    if (this.lastFullUpdate && 
        Date.now() - this.lastFullUpdate < this.config.minUpdateInterval) {
      console.log('Skipping update: too frequent');
      return;
    }

    try {
      const startTime = Date.now();
      console.log('Starting proxy fetch from all sources...');
      
      // Clear existing proxies when updating (fresh start)
      console.log(`Clearing ${this.proxies.size} existing proxies for fresh update`);
      this.proxies.clear();
      this.stats.validCount = 0;
      
      // Fetch from all sources with better error handling
      const fetchPromises = this.config.sources.map(url => 
        this.fetchFromSource(url).catch(err => {
          console.warn(`Source ${url} failed: ${err.message}`);
          return [];
        })
      );
      
      const allProxies = await Promise.allSettled(fetchPromises);
      const successfulFetches = allProxies
        .filter(result => result.status === 'fulfilled')
        .map(result => result.value)
        .flat();
      
      // Advanced deduplication and filtering
      const newProxies = this.deduplicateAndFilter(successfulFetches);
      this.stats.totalFetched = newProxies.length;
      
      console.log(`Fetched ${newProxies.length} unique proxies from ${this.config.sources.length} sources`);
      
      // Clear validation queues and add all new proxies
      this.validationQueue.length = 0;
      this.priorityQueue.length = 0;
      
      // Prioritize new proxies that aren't blacklisted
      const validNewProxies = newProxies.filter(proxy => !this.blacklist.has(proxy));
      
      // Add to appropriate queues
      validNewProxies.forEach(proxy => {
        if (this.isHighQualityProxy(proxy)) {
          this.priorityQueue.push(proxy);
        } else {
          this.validationQueue.push(proxy);
        }
      });
      
      this.lastFullUpdate = new Date();
      this.stats.lastUpdateDuration = Date.now() - startTime;
      
      console.log(`Added ${validNewProxies.length} new proxies to validation queue`);
      this.emit('proxies-fetched', validNewProxies.length);
    } catch (error) {
      console.error('Error fetching new proxies:', error.message);
    }
  }

  deduplicateAndFilter(proxies) {
    const seen = new Set();
    const filtered = [];
    
    for (const proxy of proxies) {
      if (!proxy || seen.has(proxy)) continue;
      
      const [host, port] = proxy.split(':');
      if (!host || !port || isNaN(port)) continue;
      
      const portNum = parseInt(port);
      if (portNum <= 0 || portNum > 65535) continue;
      
      // Filter out private/local IPs
      if (this.isPrivateIP(host)) continue;
      
      seen.add(proxy);
      filtered.push(proxy);
    }
    
    return filtered;
  }

  isPrivateIP(ip) {
    const parts = ip.split('.').map(Number);
    if (parts.length !== 4 || parts.some(p => isNaN(p) || p < 0 || p > 255)) return false;
    
    // Check for private ranges
    return (
      parts[0] === 10 ||
      (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) ||
      (parts[0] === 192 && parts[1] === 168) ||
      parts[0] === 127 ||
      parts[0] === 0
    );
  }

  isHighQualityProxy(proxy) {
    // Heuristics for potentially better proxies
    const [host, port] = proxy.split(':');
    const portNum = parseInt(port);
    
    // Common proxy ports are often more reliable
    const commonPorts = [3128, 8080, 8888, 1080, 80, 443, 8000, 3129];
    return commonPorts.includes(portNum);
  }

  async fetchFromSource(url) {
    try {
      const response = await axios.get(url, { 
        timeout: 15000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
      });
      
      return response.data
        .split(/[\n\r]+/)
        .map(p => p.trim())
        .filter(p => p && p.includes(':'));
    } catch (error) {
      throw new Error(`Failed to fetch from ${url}: ${error.message}`);
    }
  }

  startValidationWorker() {
    const processQueue = async () => {
      // Process priority queue first
      if (this.priorityQueue.length > 0) {
        const batch = this.priorityQueue.splice(0, Math.min(this.config.concurrency / 2, this.priorityQueue.length));
        await Promise.all(batch.map(proxy => this.validateProxy(proxy, true)));
      }
      
      // Then process regular queue
      if (this.validationQueue.length > 0) {
        const batch = this.validationQueue.splice(0, Math.min(this.config.concurrency, this.validationQueue.length));
        await Promise.all(batch.map(proxy => this.validateProxy(proxy, false)));
      }
      
      const delay = this.priorityQueue.length > 0 || this.validationQueue.length > 0 ? 200 : 2000;
      setTimeout(processQueue, delay);
    };

    // Start multiple workers for better throughput
    const workerCount = Math.min(4, Math.ceil(this.config.concurrency / 25));
    for (let i = 0; i < workerCount; i++) {
      setTimeout(() => processQueue(), i * 100);
    }
  }

  async validateProxy(proxy, isPriority = false) {
    if (this.blacklist.has(proxy)) return false;
    
    // Skip recent validations for existing proxies
    if (this.proxies.has(proxy) && !isPriority) {
      const { lastChecked } = this.proxies.get(proxy);
      if (Date.now() - lastChecked < 10 * 60 * 1000) return true;
    }

    let attempts = 0;
    const maxAttempts = isPriority ? this.config.maxRetries : 1;
    
    while (attempts < maxAttempts) {
      try {
        const success = await this.performValidation(proxy);
        if (success) {
          this.stats.successfulValidations++;
          return true;
        }
        attempts++;
      } catch (error) {
        attempts++;
        if (attempts >= maxAttempts) {
          this.handleValidationFailure(proxy);
        }
      }
    }
    
    this.stats.failedValidations++;
    return false;
  }

  async performValidation(proxy) {
    const start = Date.now();
    const [ip, port] = proxy.split(':');
    
    // Rotate validation URLs for better reliability
    const validationUrl = this.config.validationUrls[
      Math.floor(Math.random() * this.config.validationUrls.length)
    ];
    
    const agent = {
      http: new HttpProxyAgent({ 
        proxy: `http://${ip}:${port}`,
        timeout: this.config.checkTimeout
      }),
      https: new HttpsProxyAgent({ 
        proxy: `http://${ip}:${port}`,
        timeout: this.config.checkTimeout
      })
    };

    const response = await axios.get(validationUrl, {
      httpAgent: agent.http,
      httpsAgent: agent.https,
      timeout: this.config.checkTimeout,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      }
    });

    // More flexible response validation
    const hasValidResponse = response.data && (
      response.data.origin || 
      response.data.ip || 
      response.data.query ||
      (typeof response.data === 'string' && /^\d+\.\d+\.\d+\.\d+$/.test(response.data.trim()))
    );

    if (hasValidResponse && response.status === 200) {
      const latency = Date.now() - start;
      this.updateProxyStats(proxy, latency, true);
      return true;
    }
    
    return false;
  }

  updateProxyStats(proxy, latency, success) {
    const existing = this.proxies.get(proxy) || {
      latency: 0,
      successRate: 0,
      totalChecks: 0,
      lastChecked: new Date(0)
    };

    const newTotalChecks = existing.totalChecks + 1;
    const newSuccessRate = success 
      ? (existing.successRate * existing.totalChecks + 1) / newTotalChecks
      : (existing.successRate * existing.totalChecks) / newTotalChecks;

    this.proxies.set(proxy, {
      latency: success ? latency : existing.latency,
      successRate: newSuccessRate,
      totalChecks: newTotalChecks,
      lastChecked: new Date()
    });

    if (success) {
      this.stats.validCount = this.proxies.size;
      this.emit('proxy-validated', proxy, latency);
      
      // Remove from blacklist if it was there
      if (this.blacklist.has(proxy)) {
        this.blacklist.delete(proxy);
      }
    } else {
      // Remove proxy if success rate is too low
      if (newSuccessRate < this.config.minSuccessRate && newTotalChecks >= 5) {
        this.proxies.delete(proxy);
        this.blacklist.add(proxy);
        this.stats.blacklistedCount++;
      }
    }
  }

  handleValidationFailure(proxy) {
    if (this.proxies.has(proxy)) {
      this.updateProxyStats(proxy, 0, false);
    } else {
      // New proxy that failed - add to blacklist temporarily
      this.blacklist.add(proxy);
      this.stats.blacklistedCount++;
    }
  }

  revalidateExistingProxies() {
    const now = Date.now();
    const staleProxies = [];
    
    for (const [proxy, data] of this.proxies.entries()) {
      if (now - data.lastChecked > 20 * 60 * 1000) { // 20 minutes
        staleProxies.push(proxy);
      }
    }
    
    if (staleProxies.length > 0) {
      console.log(`Revalidating ${staleProxies.length} stale proxies`);
      staleProxies.forEach(proxy => this.priorityQueue.push(proxy));
    }
  }

  cleanupBlacklist() {
    const now = Date.now();
    const toRemove = [];
    
    for (const proxy of this.blacklist) {
      // This is a simplified cleanup - in a real implementation,
      // you'd want to track when items were blacklisted
      if (Math.random() < 0.01) { // Randomly give some blacklisted proxies another chance
        toRemove.push(proxy);
      }
    }
    
    toRemove.forEach(proxy => this.blacklist.delete(proxy));
    if (toRemove.length > 0) {
      console.log(`Cleaned up ${toRemove.length} blacklisted proxies`);
    }
  }

  getActiveProxies(options = {}) {
    const { maxLatency = Infinity, minSuccessRate = 0, sortBy = 'latency' } = options;
    
    let proxies = Array.from(this.proxies.entries())
      .filter(([proxy, data]) => 
        data.latency <= maxLatency && 
        data.successRate >= minSuccessRate
      );
    
    // Sort proxies
    if (sortBy === 'latency') {
      proxies.sort((a, b) => a[1].latency - b[1].latency);
    } else if (sortBy === 'successRate') {
      proxies.sort((a, b) => b[1].successRate - a[1].successRate);
    } else if (sortBy === 'random') {
      proxies = proxies.sort(() => Math.random() - 0.5);
    }
    
    return proxies.map(([proxy]) => proxy);
  }

  getProxyDetails() {
    return Array.from(this.proxies.entries()).map(([proxy, data]) => ({
      proxy,
      ...data,
      successRate: Math.round(data.successRate * 100) / 100
    }));
  }

  getStats() {
    const avgLatency = Array.from(this.proxies.values())
      .reduce((sum, p) => sum + p.latency, 0) / (this.proxies.size || 1);
    
    const avgSuccessRate = Array.from(this.proxies.values())
      .reduce((sum, p) => sum + p.successRate, 0) / (this.proxies.size || 1);

    return {
      ...this.stats,
      activeProxies: this.proxies.size,
      blacklistedProxies: this.blacklist.size,
      queueSize: this.validationQueue.length,
      priorityQueueSize: this.priorityQueue.length,
      lastFullUpdate: this.lastFullUpdate,
      averageLatency: Math.round(avgLatency),
      averageSuccessRate: Math.round(avgSuccessRate * 100) / 100,
      validationRate: this.stats.successfulValidations / (this.stats.successfulValidations + this.stats.failedValidations) || 0
    };
  }

  // Utility methods for external use
  async testProxy(proxy, timeout = 5000) {
    try {
      const [ip, port] = proxy.split(':');
      const agent = new HttpProxyAgent({ proxy: `http://${ip}:${port}` });
      
      const start = Date.now();
      const response = await axios.get('http://httpbin.org/ip', {
        httpAgent: agent,
        timeout
      });
      
      return {
        success: true,
        latency: Date.now() - start,
        response: response.data
      };
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  }

  // Graceful shutdown
  async shutdown() {
    console.log('Shutting down ProxyPool...');
    await this.persistData();
    this.removeAllListeners();
    console.log('ProxyPool shutdown complete');
  }
}

// Create and initialize proxy pool
const proxyPool = new ProxyPool();

// Express setup with enhanced middleware
const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

// Request logging middleware
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.path} - ${req.ip}`);
  next();
});

// Initialize proxy pool
proxyPool.initialize().then(() => {
  console.log('ProxyPool initialized successfully');
}).catch(error => {
  console.error('Failed to initialize ProxyPool:', error);
  process.exit(1);
});

// Enhanced API Endpoints
app.get('/proxies', (req, res) => {
  try {
    const format = req.query.format || 'json';
    const maxLatency = parseInt(req.query.maxLatency) || Infinity;
    const minSuccessRate = parseFloat(req.query.minSuccessRate) || 0;
    const sortBy = req.query.sortBy || 'latency';
    const limit = parseInt(req.query.limit) || undefined;
    
    let activeProxies = proxyPool.getActiveProxies({ 
      maxLatency, 
      minSuccessRate, 
      sortBy 
    });
    
    if (limit) {
      activeProxies = activeProxies.slice(0, limit);
    }

    if (format.toLowerCase() === 'txt') {
      res.set('Content-Type', 'text/plain');
      res.send(activeProxies.join('\n'));
    } else {
      res.json({
        status: 'success',
        count: activeProxies.length,
        lastUpdate: proxyPool.lastFullUpdate,
        filters: {
          maxLatency: maxLatency === Infinity ? null : maxLatency,
          minSuccessRate,
          sortBy,
          limit
        },
        proxies: activeProxies
      });
    }
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

app.get('/proxies/details', (req, res) => {
  try {
    const details = proxyPool.getProxyDetails();
    res.json({
      status: 'success',
      count: details.length,
      proxies: details
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

app.get('/stats', (req, res) => {
  try {
    res.json({
      status: 'success',
      ...proxyPool.getStats()
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

app.post('/test-proxy', async (req, res) => {
  try {
    const { proxy, timeout } = req.body;
    if (!proxy) {
      return res.status(400).json({
        status: 'error',
        message: 'Proxy parameter is required'
      });
    }
    
    const result = await proxyPool.testProxy(proxy, timeout);
    res.json({
      status: 'success',
      proxy,
      ...result
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: error.message
    });
  }
});

app.get('/', (req, res) => {
  const stats = proxyPool.getStats();
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>Enhanced ProxyPool API</title>
      <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
        .container { background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .stat { background: #f8f9fa; padding: 15px; border-radius: 5px; text-align: center; }
        .stat-value { font-size: 24px; font-weight: bold; color: #007bff; }
        .endpoints { background: #f8f9fa; padding: 20px; border-radius: 5px; }
        .endpoint { margin: 10px 0; }
        .endpoint a { color: #007bff; text-decoration: none; }
        .endpoint a:hover { text-decoration: underline; }
        .status { color: ${stats.activeProxies > 0 ? 'green' : 'orange'}; }
      </style>
    </head>
    <body>
      <div class="container">
        <h1>🚀 Enhanced ProxyPool API</h1>
        <p class="status">Status: <strong>${stats.activeProxies > 0 ? 'Active' : 'Initializing'}</strong></p>
        
        <div class="stats">
          <div class="stat">
            <div class="stat-value">${stats.activeProxies}</div>
            <div>Active Proxies</div>
          </div>
          <div class="stat">
            <div class="stat-value">${stats.blacklistedProxies}</div>
            <div>Blacklisted</div>
          </div>
          <div class="stat">
            <div class="stat-value">${stats.queueSize}</div>
            <div>In Queue</div>
          </div>
          <div class="stat">
            <div class="stat-value">${stats.averageLatency}ms</div>
            <div>Avg Latency</div>
          </div>
          <div class="stat">
            <div class="stat-value">${Math.round(stats.averageSuccessRate * 100)}%</div>
            <div>Avg Success Rate</div>
          </div>
        </div>

        <div class="endpoints">
          <h3>📡 API Endpoints</h3>
          <div class="endpoint">
            <strong>GET</strong> <a href="/proxies">/proxies</a> - Get active proxies (JSON)
            <br><small>Query params: format=txt|json, maxLatency, minSuccessRate, sortBy=latency|successRate|random, limit</small>
          </div>
          <div class="endpoint">
            <strong>GET</strong> <a href="/proxies/details">/proxies/details</a> - Get detailed proxy information
          </div>
          <div class="endpoint">
            <strong>GET</strong> <a href="/stats">/stats</a> - Get comprehensive statistics
          </div>
          <div class="endpoint">
            <strong>POST</strong> /test-proxy - Test a specific proxy
            <br><small>Body: {"proxy": "ip:port", "timeout": 5000}</small>
          </div>
        </div>

        <p><small>Last update: ${stats.lastFullUpdate ? new Date(stats.lastFullUpdate).toLocaleString() : 'Never'}</small></p>
      </div>
    </body>
    </html>
  `);
});

// Health check endpoint
app.get('/health', (req, res) => {
  const stats = proxyPool.getStats();
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    activeProxies: stats.activeProxies,
    timestamp: new Date().toISOString()
  });
});

// Error handling middleware
app.use((error, req, res, next) => {
  console.error('Express error:', error);
  res.status(500).json({
    status: 'error',
    message: 'Internal server error'
  });
});

// Graceful shutdown handling
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully');
  await proxyPool.shutdown();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully');
  await proxyPool.shutdown();
  process.exit(0);
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Enhanced ProxyPool API running on port ${PORT}`);
  console.log(`📊 Dashboard: http://localhost:${PORT}/`);
  console.log(`🔗 API: http://localhost:${PORT}/proxies`);
});
