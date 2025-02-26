# DE Zoomcamp HW5

Description here --> https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/05-batch/homework.md

# Setup

1. Install spark:

```bash
brew install apache-spark
# Add this to .zshrc
export SPARK_HOME="/opt/homebrew/Cellar/apache-spark/3.5.4/libexec"
export PATH="$SPARK_HOME/bin/:$PATH"
# install pyspark dependency to be able to run in a Jupyter notebook
pip install -r requirements.txt
```

2. Download data:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```

# Answers

All answers are located in the [answers folder](./answers/)

# Contact

Contact [Akshay Jain](https://www.linkedin.com/in/akshayrjain/)
