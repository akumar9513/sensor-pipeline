# How to Run & Verify

## 1. Clone & Setup
```bash
git clone https://github.com/akumar9513/sensor-pipeline.git
cd sensor-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 2. Create .env file
```bash
cat > .env << 'ENVEOF'
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_mysql_password_here
DB_NAME=sensor_pipeline
ENVEOF
```

## 3. Run the pipeline
```bash
python pipeline.py
```

## 4. Verify
Open a second terminal and run:
```bash
mysql -u root -pyour_password sensor_pipeline -e "SELECT COUNT(*) FROM sensor_readings;"
mysql -u root -pyour_password sensor_pipeline -e "SELECT COUNT(*) FROM aggregated_metrics;"
mysql -u root -pyour_password sensor_pipeline -e "SELECT COUNT(*) FROM error_log;"
ls processed/
ls quarantine/
```

## 5. Stop
Press Ctrl+C
