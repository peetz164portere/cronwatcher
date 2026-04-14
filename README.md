# cronwatcher

A lightweight CLI tool to monitor cron job execution history and alert on failures via webhook.

---

## Installation

```bash
pip install cronwatcher
```

Or install from source:

```bash
git clone https://github.com/yourname/cronwatcher.git && cd cronwatcher && pip install .
```

---

## Usage

Register a cron job to be monitored:

```bash
cronwatcher register --name "daily-backup" --schedule "0 2 * * *" --webhook https://hooks.example.com/alerts
```

Wrap your cron command to track execution:

```bash
cronwatcher run --name "daily-backup" -- /usr/local/bin/backup.sh
```

View execution history:

```bash
cronwatcher history --name "daily-backup" --last 10
```

If a job fails, `cronwatcher` will automatically POST a JSON alert payload to your configured webhook URL with the job name, exit code, timestamp, and captured output.

---

## Configuration

A `~/.cronwatcher/config.toml` file is created on first run. You can define default webhook URLs and retention settings there.

---

## License

This project is licensed under the [MIT License](LICENSE).