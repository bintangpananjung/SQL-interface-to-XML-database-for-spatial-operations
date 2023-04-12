const apps = require('./app');

const server = apps.listen(3003, () => {
  console.log(`Express is running on port ${server.address().port}`);
});