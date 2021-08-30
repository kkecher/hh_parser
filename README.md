<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <meta name="description" content="README for hh parser.">
    <meta name="author" content="Ivan Arzhanov">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="./styles.css">
  </head>
  <body>
<h2>This program can:</h2>
<ul>
  <li>Parse hh vacancies in specified regions.</li>
  <li>Store them in SQLite database.</li>
  <li>Filter vacancies.</li>
  <li>Send vacancies to Telegram.</li>
</ul>

<p>The program was tested on <code>Ubuntu 20.04</code> and <code>Windows 10</code>.</p>


<h2>Workflow</h2>
<ol>
  <li>Clone the repository into your favorite folder: <code>git clone https://github.com/kkecher/hh_parser</code>.</li>
  <li>Install <code>&gt;=Python 3.6</code>.</li>
  <li>Install Python packages:
    <ul>
      <li><code>pip install inputimeout</code></li>
      <li><code>pip install requests</code></li>
      <li><code>pip install ruamel.yaml</code></li>
    </ul>
  </li>
  <li>Create your own Telegram bot (I suppose you already have Telegram account):
    <ul>
      <li>Go to special bot <a href="https://t.me/botfather" target="_blank">BotFarther</a></li>
      <li>Send <code>/start</code></li>
      <li>Send <code>/newbot</code></li>
      <li> Send your bot name</li>
      <li> You will get token for the bot. It’s like a password &ndash; keep it secret!</li>
    </ul>
  </li>
  <li>Put your token in environment variable <code>hh_bot_token</code>:
    <ul>
      <li>Ubuntu: open <code>Terminal</code> &gt; type <code>emacs ~/.bashrc</code> &gt; press <code>ALT-SHIFT-&gt;</code> (this will move cursor to the end of file) &gt; type <code># hh_telegram_bot</code> &gt; press <code>Enter</code> &gt; type <code>export hh_bot_token={your token}</code> (paste your token in place of curly brackets. No spaces!) &gt; press <code>CTRL-x CTRL-s CTRL-x k ENTER CTRL-x CTRL-c</code> &gt; type <code>source ~/.bashrc</code> &gt; press <code >ENTER</code> &gt; type <code>echo $hh_bot_token</code>. Here we are! If you see your token &ndash; you did it!</li>
      <li>Windows: open <code>Start menu</code> &gt; type <code>variable</code> &gt; choose command <code>Edit environment variables for your account</code> &gt; press upper <code>New&hellip;</code> button &gt; <code>Variable name:</code> = <code>hh_bot_token</code>, <code>Variable value:</code> = <code>{your token}</code> &gt; press <code>OK</code> in all windows. Open <code>CMD</code> &gt; type <code>echo %hh_bot_token%</code>. If you see your token &ndash; you did it!</li>
    </ul>
  </li>
  <li> Next we need to know bot’s chat id:
    <ul>
      <li>Send any message in your bot chat.</li>
      <li>In Terminal &sol; CMD type <code>python</code> <code>ENTER</code> &gt; type <code>import requests</code> <code>ENTER</code> &gt; type <code>r = requests.get("https://api.telegram.org/{your token}/getUpdates")</code> <code>ENTER</code> &gt; type <code>r.text</code> <code>ENTER</code>. You will get dictionary, find value for key <code>["result"]["message"]["from"]["id"]</code>. That’s your chat id, put it in <code>config.yaml &gt; chat_id</code> value.
    </ul>
  </li>
  <li>Well&hellip; that’s it! In Terminal / CMD run <code>python main.py</code>. The program will ask you desirable regions and it will start to collect vacancies and send them to the Telegram bot. I have to warn you that in Moscow, for example, you will get more than 9000 vacancies A DAY. So use filters, for more information and examples see <code>config.yaml</code></li>
  <li>Certainly, we want to automate this routine:
    <ul>
      <li>Ubuntu: open <code>Terminal</code> &gt; type <code>sudo apt update</code> <code>ENTER</code> &gt; type your sudo password <code>ENTER</code>&gt; type <code>sudo apt-get install cron</code> <code>ENTER</code> &gt; type <code>service cron start</code> &gt; type <code>crontab -e</code> <code>ENTER</code> &gt; choose the best text editor in appeared menu if you haven’t set cron before &gt; add a new line to the file end and type <code>*/5 * * * * cd "{path to the project}" && "/usr/bin/python" "main.py"</code> <code>ENTER</code> &gt; save and exit to Terminal &gt; type <code>crontab -l</code> <code>ENTER</code> to check if settings have been applied &gt; type <code>service cron status</code> <code>ENTER</code> to check if cron is running. If everything was set correctly you will get new messages every 5 minutes.</li>
      <li>Windows: open file <code>hh_parser.bat</code> and change project and Python paths to yours (you can see Python path in CMD with command <code>which python</code>) &gt; save and close file &gt; open <code>Start menu</code> &gt; type <code>task schedule</code> &gt; choose command <code>Task Scheduler</code> &gt; choose command <code>Create Task&hellip;</code> &gt; on <code>General</code> tab type any task name &gt; on <code>Triggers</code> tab click <code>New&hellip;</code> &gt; choose <code>Daily</code>, <code>Start: {date and time when you want the task to start}</code>, <code>Advanced settings &gt; Repeat task every: 5 minutes &gt; for a duration of: Indefinitely</code> &gt; on <code>Actions</code> tab click <code>New&hellip;</code> &gt; <code>Program/script: %windir%\system32\cmd.exe &gt; Add arguments (optional): /C start "" /MIN {path to the project}\hh_parser.bat</code> &gt; press <code>OK</code>. That’s it!</li>
      <li>Everything was set correctly if <code>config.yaml &gt; url_params &gt; date_from</code> value changes every 5 minutes.</li>
      <li>If you decide to change time interval, it’s highly recommended to set the interval to be more than <code>config.yaml &gt; kill_program_after</code> value to avoid overlapping sessions.</li>
    </ul>
  </li>
</ol>
<p>Good luck finding your dream job!</p>
  </body>
</html>
