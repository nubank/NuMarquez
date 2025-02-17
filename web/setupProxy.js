
const express = require('express')
const { createProxyMiddleware } = require('http-proxy-middleware')
const path = require('path')
const appMetrics = require('./services/appMetrics')
const { sendLogToKafka } = require('./services/kafkaProducer')
const { getFormattedDateTime } = require('./services/dateTimeHelper')

const app = express();
const router = express.Router();
const distPath = path.join(__dirname, 'dist')

// Initialize Metrics
const metrics = new appMetrics();

// Middleware to expose /metrics endpoint
app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', metrics.register.contentType);
    res.end(await metrics.getMetrics());
  } catch (ex) {
    res.status(500).end(ex);
  }
});

const { connectProducer } = require('./services/kafkaProducer');

(async () => {
  try {
    await connectProducer();
  } catch (error) {
    console.error('Error connecting Kafka producer:', error);
    process.exit(1);
  }
})();

const environmentVariable = (variableName) => {
  const value = process.env[variableName]
  if (!value) {
      console.error(`Error: ${variableName} environment variable is not defined.`)
      console.error(`Please set ${variableName} and restart the application.`)
      process.exit(1)
  }
  return value
}

const port = environmentVariable("WEB_PORT")

const apiOptions = {
  target: `http://${environmentVariable("MARQUEZ_HOST")}:${environmentVariable("MARQUEZ_PORT")}/`,
  changeOrigin: true,
}

app.use(express.json());

// Serve static files for specific routes
app.use('/', express.static(distPath))
app.use('/datasets', express.static(distPath))
app.use('/events', express.static(distPath))
app.use('/lineage', express.static(distPath))
app.use('/jobs', express.static(distPath))
app.use('/datasets/column-level', express.static(distPath))

// Proxy API requests
+app.use('/api/v1', createProxyMiddleware(apiOptions))
app.use('/api/v2beta', createProxyMiddleware(apiOptions))

// Healthcheck route
router.get('/healthcheck', (req, res) => {
  res.send('OK')
})

app.use(router)

// **Catch-All Route to Serve index.html for Client-Side Routing**
app.get('*', (req, res) => {
  res.sendFile(path.join(distPath, 'index.html'))
})

app.listen(port, () => {
  console.log(`App listening on port ${port}!`)
})

app.use(express.json());

const { buildLogData } = require('./services/logFormatter');
const { userInfo, userInfo, userInfo } = require('os')

// Endpoint to log user info and increment counters
app.post('/api/loguserinfo', (req, res) => {
  const { email = '' } = req.body;

  // Guard against invalid email values
  if (typeof email !== 'string') {
    return res.status(400).json({ error: 'Invalid email format' });
  }

  // Define a minimal userInfo object with email
  const userInfo = {
    email,
    name: userInfo.name,  
    locale: userInfo.locale,  
    zoneinfo: userInfo.zoneinfo,
    email_verified: true
  };

  // Build enriched log data using the helper
  const kafkaData = buildLogData(userInfo);

  // Encode the email for your own logs
  const encodedEmail = Buffer.from(email).toString('base64');

  // Update metrics
  metrics.incrementTotalLogins();
  metrics.incrementUniqueLogins(email);

  // Console log for local debugging
  const logData = {
    accessLog: {
      email: encodedEmail,
      dateTime: getFormattedDateTime()
    }
  };
  console.log(JSON.stringify(logData));

  // Send meta info to Kafka
  sendLogToKafka(kafkaData);

  // Response
  res.sendStatus(200);
});

module.exports = app;

