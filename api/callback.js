export default function handler(req, res) {
  const { code, state, error } = req.query;

  if (error) {
    return res.send(errorPage(error));
  }

  if (!code || !state) {
    return res.send(errorPage('Missing authorization code'));
  }

  return res.send(successPage(code, state));
}

function successPage(code, state) {
  // Encode both state and code so bot can exchange for token
  const payload = Buffer.from(JSON.stringify({ s: state, c: code })).toString('base64');
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Spotify Connected</title>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700&display=swap" rel="stylesheet">
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      min-height: 100vh;
      font-family: 'Outfit', sans-serif;
      background: linear-gradient(135deg, #0d0d0d 0%, #1a1a2e 50%, #16213e 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
    }
    .container { text-align: center; max-width: 420px; }
    .icon-container {
      width: 120px; height: 120px;
      margin: 0 auto 32px;
      background: linear-gradient(135deg, #1DB954 0%, #1ed760 100%);
      border-radius: 50%;
      display: flex; align-items: center; justify-content: center;
      box-shadow: 0 20px 60px rgba(29, 185, 84, 0.4);
      animation: pulse 2s ease-in-out infinite;
    }
    @keyframes pulse {
      0%, 100% { transform: scale(1); }
      50% { transform: scale(1.05); }
    }
    .icon-container svg { width: 60px; height: 60px; fill: white; }
    h1 { color: #fff; font-size: 2rem; font-weight: 700; margin-bottom: 12px; }
    .subtitle { color: #1DB954; font-size: 1.1rem; font-weight: 600; margin-bottom: 24px; }
    .message { color: rgba(255, 255, 255, 0.7); font-size: 1rem; line-height: 1.6; margin-bottom: 24px; }
    .card {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: 16px; padding: 24px;
      margin-bottom: 16px;
    }
    .card-title { color: rgba(255, 255, 255, 0.5); font-size: 0.85rem; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 12px; }
    .instruction { color: #fff; font-size: 1.1rem; font-weight: 600; }
    .code-box {
      background: rgba(29, 185, 84, 0.2);
      border: 1px solid #1DB954;
      border-radius: 8px;
      padding: 12px;
      margin: 16px 0;
      word-break: break-all;
      font-family: monospace;
      font-size: 0.75rem;
      color: #1DB954;
      cursor: pointer;
      transition: all 0.2s;
    }
    .code-box:hover { background: rgba(29, 185, 84, 0.3); }
    .code-box:active { transform: scale(0.98); }
    .copy-hint { color: rgba(255,255,255,0.5); font-size: 0.8rem; margin-top: 8px; }
    .checkmark { color: #1DB954; margin-right: 8px; }
  </style>
</head>
<body>
  <div class="container">
    <div class="icon-container">
      <svg viewBox="0 0 24 24"><path d="M12 0C5.4 0 0 5.4 0 12s5.4 12 12 12 12-5.4 12-12S18.66 0 12 0zm5.521 17.34c-.24.359-.66.48-1.021.24-2.82-1.74-6.36-2.101-10.561-1.141-.418.122-.779-.179-.899-.539-.12-.421.18-.78.54-.9 4.56-1.021 8.52-.6 11.64 1.32.42.18.479.659.301 1.02zm1.44-3.3c-.301.42-.841.6-1.262.3-3.239-1.98-8.159-2.58-11.939-1.38-.479.12-1.02-.12-1.14-.6-.12-.48.12-1.021.6-1.141C9.6 9.9 15 10.561 18.72 12.84c.361.181.54.78.241 1.2zm.12-3.36C15.24 8.4 8.82 8.16 5.16 9.301c-.6.179-1.2-.181-1.38-.721-.18-.601.18-1.2.72-1.381 4.26-1.26 11.28-1.02 15.721 1.621.539.3.719 1.02.419 1.56-.299.421-1.02.599-1.559.3z"/></svg>
    </div>
    <h1>Almost Done!</h1>
    <p class="subtitle">Spotify authorized</p>
    <p class="message">Copy the command below and paste it in your chat to complete the link.</p>
    <div class="card">
      <p class="card-title">Copy & Paste This</p>
      <div class="code-box" onclick="navigator.clipboard.writeText('/spotifylink ${payload}'); this.innerHTML='Copied!'; setTimeout(() => this.innerHTML='/spotifylink ...', 1500)">
        /spotifylink ${payload.substring(0, 40)}...
      </div>
      <p class="copy-hint">Tap to copy</p>
    </div>
  </div>
</body>
</html>`;
}

function errorPage(error) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Connection Failed</title>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700&display=swap" rel="stylesheet">
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      min-height: 100vh;
      font-family: 'Outfit', sans-serif;
      background: linear-gradient(135deg, #0d0d0d 0%, #1a1a2e 50%, #16213e 100%);
      display: flex; align-items: center; justify-content: center; padding: 20px;
    }
    .container { text-align: center; max-width: 420px; }
    .icon-container {
      width: 120px; height: 120px; margin: 0 auto 32px;
      background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
      border-radius: 50%; display: flex; align-items: center; justify-content: center;
    }
    .icon-container span { font-size: 48px; color: white; }
    h1 { color: #fff; font-size: 2rem; margin-bottom: 12px; }
    .message { color: rgba(255, 255, 255, 0.7); font-size: 1rem; line-height: 1.6; }
  </style>
</head>
<body>
  <div class="container">
    <div class="icon-container"><span>✕</span></div>
    <h1>Connection Failed</h1>
    <p class="message">${error}<br><br>Go back to chat and try <strong>/spotify</strong> again.</p>
  </div>
</body>
</html>`;
}

