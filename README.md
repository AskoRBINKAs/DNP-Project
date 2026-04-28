# DNP Project - Distributed Logging and Monitoring System: centralized view from distributed nodes

## Repository structure
```
.
├── docker-compose.central_node.yml   # Docker Compose configuration for central node
├── setup_central_node.sh             # Central node installation script
├── env.rabbitmq.example              # Example RabbitMQ environment file
├── grafana/                          # Grafana provisioning files
├── log_collector/                    # Log collector service
├── log_producer/                     # Client-side log producer
└── manticore/                        # ManticoreSearch configuration and SQL schema
```

## Central node
The central node runs the main infrastructure components:

* RabbitMQ
* ManticoreSearch
* Log Collector
* Grafana

### Requirements

- Docker Engine v28+
- Docker Compose plugin
- Linux server or VM

Tested on Ubuntu 20.04 LTS.

### Installation

To setup central node you need run installation script:
```bash
git clone https://github.com/AskoRBINKAs/DNP-Project.git
cd DNP-Project
bash setup_central_node.sh
```
The script will:

- Generate credentials for the log collector user.
- Create .env.collector.
- Start RabbitMQ and ManticoreSearch.
- Create the required ManticoreSearch table.
- Configure RabbitMQ virtual host and user.
- Build and start the log collector.
- Start the full central node stack.

After installation, the generated RabbitMQ password for the logger user will be printed in the terminal and saved in .env.collector.

### Post installation

After executing installation script you need finish Grafana setup. Open `<IP>:3000` in browser, login as `admin:admin` and set new administrator password. Next, import dashboard from `grafana/dashboards/log-dashboard.json` file.

## Client node

### Installation

Install requirements:

```bash
pip3 install -r log_producer/requirements.txt
```

### How to run

To run client:
```bash
python3 log_producer/producer.py --tag <TAG> --source_ip <IP> --interval <INTERVAL> --rabbitmq-url <AMQP-STRING>
```

Options:
* `--tag` - tag to display in dashboard
* `--source_ip` - ip to display in dashboard
* `--interval` - delay between log generation
* `--rabbitmq-url` - AMQP connection string

Example:
```bash
python3 log_producer/producer.py \
  --tag web-server-01 \
  --ip 192.168.1.20 \
  --interval 5 \
  --rabbitmq-url "amqp://logger:my-secret-password@192.168.1.10:5672/logs"
```