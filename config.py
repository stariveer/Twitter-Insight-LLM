import os
from dotenv import load_dotenv

# 加载.env文件中的环境变量
load_dotenv()

# 从环境变量中获取配置
TWITTER_AUTH_TOKEN = os.getenv('TWITTER_AUTH_TOKEN')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
START_DATE = os.getenv('START_DATE')
END_DATE = os.getenv('END_DATE')
TWITTER_URL = os.getenv('TWITTER_URL')