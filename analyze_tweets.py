import json
import os
from datetime import datetime

def analyze_tweet_file(file_path):
    tweets = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                tweet = json.loads(line)
                tweets.append(tweet)
            except json.JSONDecodeError:
                continue
    
    if not tweets:
        print(f"文件 {file_path} 中没有有效的推文数据")
        return
    
    dates = [tweet.get("date") for tweet in tweets if tweet.get("date")]
    
    print(f"文件: {os.path.basename(file_path)}")
    print(f"总推文数: {len(tweets)}")
    if dates:
        print(f"最早推文日期: {min(dates)}")
        print(f"最晚推文日期: {max(dates)}")
    print("-" * 50)

def main():
    data_dir = "data"
    for file in os.listdir(data_dir):
        if file.endswith(".json") and file.startswith("elonmusk@"):
            analyze_tweet_file(os.path.join(data_dir, file))

if __name__ == "__main__":
    main() 