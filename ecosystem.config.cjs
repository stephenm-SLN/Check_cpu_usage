/**
 * PM2 ecosystem config for gui_cpu_usage Flask app.
 * Run: pm2 start ecosystem.config.cjs
 *
 * For production, install gunicorn and use the gunicorn command below.
 * Otherwise the app runs with Flask's built-in server (debug off).
 */
module.exports = {
  apps: [{
    name: 'cpu-usage-gui',
    script: 'gui_cpu_usage.py',
    interpreter: 'python',
    // Or use gunicorn for production: interpreter: 'none', script: 'gunicorn', args: '-w 2 -b 0.0.0.0:8456 gui_cpu_usage:app'
    env: {
      FLASK_DEBUG: '0',
    },
    cwd: __dirname,
  }],
};
