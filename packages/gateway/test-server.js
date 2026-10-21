const Fastify = require('fastify');
const app = Fastify();
app.get('/health', async () => ({ status: 'ok' }));
app.listen({ port: 8002, host: '0.0.0.0' })
  .then(addr => console.log('Server listening on:', addr))
  .catch(err => console.error('Error:', err));