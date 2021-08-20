<h2>This program can:</h2>
<ul>
  <li>Parse hh vacancies in specified regions.</li>
  <li>Store them in sqlite database.</li>
  <li>Filter vacancies.</li>
  <li>Send vacancies to Telegram.</li>
</ul>

<h2>Workflow</h2>
<ol>
  <li>The program was tested on <code>Ubuntu 20.04</code> and <code>Windows 10</code>.</li>
  <li>Clone the repository in your favorite folder: <code>git clone https://github.com/kkecher/hh_parser</code></li>
  <li>Install <code>&raquo;=Python 3.6 (<a href="https://www.anaconda.com/products/individual" target="_blank">Anaconda</a> might be a good choice)</li>
  <li>Install Python packages:
    <ul>
      <li><code>pip install inputimeout</code></li>
      <li><code>pip install requests</code></li>
      <li><code>pip install ruamel.yaml</code></li>
    </ul>
  </li>
  <li>Create your own Telegram bot (I suppose you have Telegram account already):
    <ul>
      <li>To create a bot you need a&hellip; bot. Go to <a href="https://t.me/botfather" target="_blank">BotFarther</a></li>
      <li>Send <code>/start</code></li>
      <li>Send <code>/newbot</code></li>
      <li> Name your bot and sent it</li>
      <li> You will get token for you bot. It’s like a password &mdash; keep it secret!</li>
      <li>Put your token in environment variable <code>hh_bot_token</code>:
	<ul>
	  <li>Ubuntu: open <code>Terminal</code> &raquo; type <code>emacs ~/.bashrc</code> &raquo; press <code>ALT-SHIFT-&raquo;</code> (this will move cursor to end of file) &raquo; type <code># hh_telegram_bot</code> and press <code>Enter</code> &raquo; type <code>export hh_bot_token={your token}</code> (paste your token in place of curly brackets. No spaces!) &raquo; press <code>CTRL-x CTRL-s CTRL-x k ENTER CTRL-x CTRL-c</code> &raquo; type <code>source ~/.bashrc</code> and press <code >ENTER</code> &raquo type <code>echo $hh_bot_token</code>. Here we are! If you see your token, you did it!</li>
	  <li>Windows 10: open <code>Start menu</code> &raquo; type <code>variable</code> and choose <code>Edit environment variables for your account</code> &raquo; press upper <code>New&hellip;</code> button, <code>Variable name:</code> = <code>hh_bot_token</code>, <code>Variable value:</code> = <code>{your token}</code> &raquo; open <code>CMD</code> and type <code>echo %hh_bot_token%</code>. If you see your token &mdash; you did it!</li>
	</ul>
      </li>
      <li> Next we need to know out bot’s chat id. In Terminal / CMD start <code>python</code> &raquo; type <code>import requests</code> &raquo; type <code>r = requests.get("https://api.telegram.org/bot{your token}/getUpdates") &raquo; type r.text
	  xTBC
    </ul>
  </li>
  <li>Open Terminal / CMD and go to the program folder</li>
  <li>Run <code>python main.py</code></li>
  <li>This will create database in <code>./data</code> folder and parse hh and vacancies. The program will terminate with error. 
</ol>
