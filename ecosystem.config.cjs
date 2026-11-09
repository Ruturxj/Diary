module.exports = {
  apps: [
    {
      name: "personal-diary",
      script: "./server.mjs",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "512M",
      env: {
        NODE_ENV: "production",
        PORT: 3000
      }
    }
  ]
};
