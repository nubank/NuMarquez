const redis = require('redis');

// Function to create a Redis client
const createRedisClient = (host, port, role) => {
  const client = redis.createClient({
    url: `redis://${host}:${port}`,
  });

  client.on('error', (err) => {
    console.error(`Redis ${role} client error:`, err);
  });

  client.on('connect', () => {
    console.log(`Connected to Redis ${role} client`);
  });

  return client;
};

// Create Redis write and read clients
const redisWriteClient = createRedisClient(
  process.env.REDIS_WRITE_HOST || 'prod-global-1-marque-ro.tcq6zg.ng.0001.use1.cache.amazonaws.com',
  process.env.REDIS_PORT || 6379,
  'write'
);

const redisReadClient = createRedisClient(
  process.env.REDIS_READ_HOST || 'prod-global-1-marque-ro.tcq6zg.ng.0001.use1.cache.amazonaws.com',
  process.env.REDIS_PORT || 6379,
  'read'
);

// Connect the clients once
(async () => {
  try {
    await redisWriteClient.connect();
    await redisReadClient.connect();
    console.log('Redis clients connected successfully.');
  } catch (err) {
    console.error('Error connecting to Redis:', err);
    process.exit(1);
  }
})();

module.exports = {
  redisWriteClient,
  redisReadClient,
};