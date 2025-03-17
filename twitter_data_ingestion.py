# -*- coding: utf-8 -*-
"""
Twitter 数据抓取脚本
功能：通过 Selenium 模拟浏览器操作，抓取 Twitter 上指定用户或话题的推文数据，并保存为 JSON 和 Excel 文件。
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime, timedelta
import re
import json
import time
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import logging
import os
from dotenv import load_dotenv

# 加载.env文件中的环境变量，但不覆盖已存在的环境变量
load_dotenv(override=False)

# 从环境变量中获取配置
TWITTER_AUTH_TOKEN = os.getenv('TWITTER_AUTH_TOKEN')  # Twitter 认证令牌
START_DATE = os.getenv('START_DATE', '2023-01-01')  # 数据抓取的开始日期，默认为2023-01-01
END_DATE = os.getenv('END_DATE', '2023-01-02')  # 数据抓取的结束日期，默认为2023-01-02
TWITTER_URL = os.getenv('TWITTER_URL', 'https://x.com/search?q=(from%3Aelonmusk)%20until%3A2023-01-02%20since%3A2023-01-01&src=typed_query&f=live')  # Twitter 搜索页面的 URL

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 确保数据目录存在
os.makedirs('data', exist_ok=True)


class TwitterExtractor:
    """
    Twitter 数据抓取类
    功能：模拟浏览器操作，抓取 Twitter 页面上的推文数据。
    """
    def __init__(self, headless=True):
        """
        初始化 TwitterExtractor 实例。
        参数：
            headless (bool): 是否以无头模式运行浏览器，默认为 True。
        """
        self.driver = self._start_chrome(headless)  # 启动 Chrome 浏览器
        self.set_token()  # 设置 Twitter 认证令牌

    def _start_chrome(self, headless):
        """
        启动 Chrome 浏览器。
        参数：
            headless (bool): 是否以无头模式运行浏览器。
        返回：
            driver: Selenium WebDriver 实例。
        """
        options = Options()
        options.headless = headless
        driver = webdriver.Chrome(options=options)
        driver.get("https://x.com")
        return driver

    def set_token(self, auth_token=TWITTER_AUTH_TOKEN):
        """
        设置 Twitter 认证令牌。
        参数：
            auth_token (str): Twitter 认证令牌。
        """
        if not auth_token or auth_token == "YOUR_TWITTER_AUTH_TOKEN_HERE":
            raise ValueError("访问令牌缺失。请正确配置它。")
        expiration = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        cookie_script = f"document.cookie = 'auth_token={auth_token}; expires={expiration}; path=/';"
        self.driver.execute_script(cookie_script)

    def fetch_tweets(self, page_url, start_date, end_date):
        """
        抓取指定页面上的推文数据。
        参数：
            page_url (str): Twitter 页面的 URL。
            start_date (str): 数据抓取的开始日期，格式为 YYYY-MM-DD。
            end_date (str): 数据抓取的结束日期，格式为 YYYY-MM-DD。
        """
        self.driver.get(page_url)
        
        # 初始化变量用于跟踪作者和日期范围
        author_handles = set()
        min_date = None
        max_date = None
        tweets_data = []

        # 将start_date和end_date从"YYYY-MM-DD"转换为datetime对象
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        while True:
            tweet = self._get_first_tweet()
            if not tweet:
                continue

            row = self._process_tweet(tweet)
            if row["date"]:
                try:
                    # 解析完整的ISO格式日期时间
                    date = datetime.fromisoformat(row["date"].replace('Z', '+00:00'))
                    
                    # 更新最早和最晚的日期
                    if min_date is None or date < min_date:
                        min_date = date
                    if max_date is None or date > max_date:
                        max_date = date
                        
                except ValueError as e:
                    logger.info(f"日期格式值错误: {row['date']}", e)
                    # 如果无法解析日期，跳过此推文
                    self._delete_first_tweet()
                    continue

                if date.date() < start_date.date():
                    break
                elif date.date() > end_date.date():
                    self._delete_first_tweet()
                    continue

            # 收集作者handle
            if row["author_handle"]:
                # 去掉@符号
                clean_handle = row["author_handle"].replace("@", "")
                author_handles.add(clean_handle)
            
            # 保存推文数据到列表
            tweets_data.append(row)
            
            logger.info(
                f"处理推文...\n{row['date']},  {row['author_name']} -- {row['text'][:50]}...\n\n"
            )
            self._delete_first_tweet()
        
        # 根据作者handle和日期范围生成文件名
        if len(author_handles) == 1:
            # 单一作者
            prefix = next(iter(author_handles))
        else:
            # 多个作者
            prefix = "multi"
        
        # 格式化日期范围
        date_range = ""
        if min_date and max_date:
            min_date_str = min_date.strftime('%Y-%m-%d')
            max_date_str = max_date.strftime('%Y-%m-%d')
            date_range = f"{min_date_str}_{max_date_str}"
        else:
            # 如果没有日期信息，使用当前时间
            date_range = datetime.now().strftime('%Y-%m-%d')
        
        # 生成最终文件名
        cur_filename = f"data/{prefix}@{date_range}"
        
        # 保存所有推文到JSON文件
        with open(f"{cur_filename}.json", "w", encoding="utf-8") as file:
            for tweet in tweets_data:
                json.dump(tweet, file)
                file.write("\n")
        
        # 保存到Excel
        self._save_to_excel(
            json_filename=f"{cur_filename}.json", output_filename=f"{cur_filename}.xlsx"
        )

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(TimeoutException),
    )
    def _get_first_tweet(
        self, timeout=10, use_hacky_workaround_for_reloading_issue=True
    ):
        """
        获取页面上的第一条推文。
        参数：
            timeout (int): 等待超时时间，默认为 10 秒。
            use_hacky_workaround_for_reloading_issue (bool): 是否使用 hacky workaround 解决页面加载问题，默认为 True。
        返回：
            tweet: 页面上的第一条推文元素。
        """
        try:
            # 等待推文或错误信息出现
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.find_elements(By.XPATH, "//article[@data-testid='tweet']")
                or d.find_elements(By.XPATH, "//span[contains(text(),'Try reloading')]")
            )

            # 检查错误信息并尝试点击 "Retry"
            error_message = self.driver.find_elements(
                By.XPATH, "//span[contains(text(),'Try reloading')]"
            )
            if error_message and use_hacky_workaround_for_reloading_issue:
                logger.info(
                    "Encountered 'Something went wrong. Try reloading.' error.\nTrying to resolve with a hacky workaround (click on another tab and switch back). Note that this is not optimal.\n"
                )
                logger.info(
                    "You do not have to worry about data duplication though. The save to excel part does the dedup."
                )
                self._navigate_tabs()

                WebDriverWait(self.driver, timeout).until(
                    lambda d: d.find_elements(
                        By.XPATH, "//article[@data-testid='tweet']"
                    )
                )
            elif error_message and not use_hacky_workaround_for_reloading_issue:
                raise TimeoutException(
                    "Error message present. Not using hacky workaround."
                )

            else:
                # 如果没有错误信息，返回第一条推文
                return self.driver.find_element(
                    By.XPATH, "//article[@data-testid='tweet']"
                )

        except TimeoutException:
            logger.error("Timeout waiting for tweet or after clicking 'Retry'")
            raise
        except NoSuchElementException:
            logger.error("Could not find tweet or 'Retry' button")
            raise

    def _navigate_tabs(self, target_tab="Likes"):
        """
        切换页面标签以解决页面加载问题。
        参数：
            target_tab (str): 目标标签，默认为 "Likes"。
        """
        try:
            # 点击 "Media" 标签
            self.driver.find_element(By.XPATH, "//span[text()='Media']").click()
            time.sleep(2)  # 等待标签加载

            # 切换回目标标签
            self.driver.find_element(By.XPATH, f"//span[text()='{target_tab}']").click()
            time.sleep(2)  # 等待目标标签加载
        except NoSuchElementException as e:
            logger.error("Error navigating tabs: " + str(e))

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(1))
    def _process_tweet(self, tweet):
        """
        处理单条推文，提取推文数据。
        参数：
            tweet: 推文元素。
        返回：
            data (dict): 提取的推文数据。
        """
        author_name, author_handle = self._extract_author_details(tweet)
        try:
            data = {
                "text": self._get_element_text(
                    tweet, ".//div[@data-testid='tweetText']"
                ),
                "author_name": author_name,
                "author_handle": author_handle,
                "date": self._get_element_attribute(tweet, "time", "datetime"),
                "lang": self._get_element_attribute(
                    tweet, "div[data-testid='tweetText']", "lang"
                ),
                "url": self._get_tweet_url(tweet),
                "mentioned_urls": self._get_mentioned_urls(tweet),
                "is_retweet": self.is_retweet(tweet),
                "media_type": self._get_media_type(tweet),
                "images_urls": (
                    self._get_images_urls(tweet)
                    if self._get_media_type(tweet) == "Image"
                    else None
                ),
            }
        except Exception as e:
            logger.error(f"Error processing tweet: {e}")
            logger.info(f"Tweet: {tweet}")
            raise

        # 提取 aria-label 中的数字
        data.update(
            {
                "num_reply": self._extract_number_from_aria_label(tweet, "reply"),
                "num_retweet": self._extract_number_from_aria_label(tweet, "retweet"),
                "num_like": self._extract_number_from_aria_label(tweet, "like"),
            }
        )
        return data

    def _get_element_text(self, parent, selector):
        """
        获取元素的文本内容。
        参数：
            parent: 父元素。
            selector (str): 元素的选择器。
        返回：
            text (str): 元素的文本内容，如果找不到元素则返回空字符串。
        """
        try:
            return parent.find_element(By.XPATH, selector).text
        except NoSuchElementException:
            return ""

    def _get_element_attribute(self, parent, selector, attribute):
        """
        获取元素的属性值。
        参数：
            parent: 父元素。
            selector (str): 元素的选择器。
            attribute (str): 属性名。
        返回：
            attribute_value (str): 元素的属性值，如果找不到元素则返回空字符串。
        """
        try:
            return parent.find_element(By.CSS_SELECTOR, selector).get_attribute(
                attribute
            )
        except NoSuchElementException:
            return ""

    def _get_mentioned_urls(self, tweet):
        """
        获取推文中提到的链接。
        参数：
            tweet: 推文元素。
        返回：
            urls (list): 提到的链接列表。
        """
        try:
            # 查找所有可能包含链接的 'a' 标签
            link_elements = tweet.find_elements(
                By.XPATH, ".//a[contains(@href, 'http')]"
            )
            urls = [elem.get_attribute("href") for elem in link_elements]
            return urls
        except NoSuchElementException:
            return []

    def is_retweet(self, tweet):
        """
        检查推文是否为转发。
        参数：
            tweet: 推文元素。
        返回：
            is_retweet (bool): 是否为转发。
        """
        try:
            # 查找是否包含 "Retweeted" 文本
            retweet_indicator = tweet.find_element(
                By.XPATH, ".//div[contains(text(), 'Retweeted')]"
            )
            if retweet_indicator:
                return True
        except NoSuchElementException:
            return False

    def _get_tweet_url(self, tweet):
        """
        获取推文的 URL。
        参数：
            tweet: 推文元素。
        返回：
            url (str): 推文的 URL，如果找不到则返回空字符串。
        """
        try:
            link_element = tweet.find_element(
                By.XPATH, ".//a[contains(@href, '/status/')]"
            )
            return link_element.get_attribute("href")
        except NoSuchElementException:
            return ""

    def _extract_author_details(self, tweet):
        """
        提取推文作者的详细信息。
        参数：
            tweet: 推文元素。
        返回：
            author_name (str): 作者名称。
            author_handle (str): 作者的 Twitter 账号。
        """
        author_details = self._get_element_text(
            tweet, ".//div[@data-testid='User-Name']"
        )
        # 按换行符分割字符串
        parts = author_details.split("\n")
        if len(parts) >= 2:
            author_name = parts[0]
            author_handle = parts[1]
        else:
            # 如果格式不符合预期，使用默认值
            author_name = author_details
            author_handle = ""

        return author_name, author_handle

    def _get_media_type(self, tweet):
        """
        获取推文的媒体类型。
        参数：
            tweet: 推文元素。
        返回：
            media_type (str): 媒体类型，可能是 "Video"、"Image" 或 "No media"。
        """
        if tweet.find_elements(By.CSS_SELECTOR, "div[data-testid='videoPlayer']"):
            return "Video"
        if tweet.find_elements(By.CSS_SELECTOR, "div[data-testid='tweetPhoto']"):
            return "Image"
        return "No media"

    def _get_images_urls(self, tweet):
        """
        获取推文中的图片链接。
        参数：
            tweet: 推文元素。
        返回：
            images_urls (list): 图片链接列表。
        """
        images_urls = []
        images_elements = tweet.find_elements(
            By.XPATH, ".//div[@data-testid='tweetPhoto']//img"
        )
        for image_element in images_elements:
            images_urls.append(image_element.get_attribute("src"))
        return images_urls

    def _extract_number_from_aria_label(self, tweet, testid):
        """
        从 aria-label 中提取数字。
        参数：
            tweet: 推文元素。
            testid (str): aria-label 的测试 ID。
        返回：
            number (int): 提取的数字，如果找不到则返回 0。
        """
        try:
            text = tweet.find_element(
                By.CSS_SELECTOR, f"div[data-testid='{testid}']"
            ).get_attribute("aria-label")
            numbers = [int(s) for s in re.findall(r"\b\d+\b", text)]
            return numbers[0] if numbers else 0
        except NoSuchElementException:
            return 0

    def _delete_first_tweet(self, sleep_time_range_ms=(0, 1000)):
        """
        删除页面上的第一条推文。
        参数：
            sleep_time_range_ms (tuple): 删除后等待的时间范围（毫秒），默认为 (0, 1000)。
        """
        try:
            tweet = self.driver.find_element(
                By.XPATH, "//article[@data-testid='tweet'][1]"
            )
            self.driver.execute_script("arguments[0].remove();", tweet)
        except NoSuchElementException:
            logger.info("Could not find the first tweet to delete.")

    @staticmethod
    def _save_to_excel(json_filename, output_filename="data/data.xlsx"):
        """
        将 JSON 数据保存为 Excel 文件。
        参数：
            json_filename (str): JSON 文件路径。
            output_filename (str): 输出的 Excel 文件路径。
        """
        # 读取 JSON 数据
        cur_df = pd.read_json(json_filename, lines=True)

        # 去重并保存到 Excel
        cur_df.drop_duplicates(subset=["url"], inplace=True)
        cur_df.to_excel(output_filename, index=False)
        logger.info(
            f"\n\nDone saving to {output_filename}. Total of {len(cur_df)} unique tweets."
        )


if __name__ == "__main__":
    # 启动 TwitterExtractor 实例
    scraper = TwitterExtractor()
    # 抓取推文数据
    scraper.fetch_tweets(
        TWITTER_URL,
        start_date=START_DATE,
        end_date=END_DATE,
    )  # YYYY-MM-DD格式

    # 如果你只想导出到Excel，可以使用以下行
    # scraper._save_to_excel(json_filename="tweets_2024-02-01_14-30-00.json", output_filename="tweets_2024-02-01_14-30-00.xlsx")
