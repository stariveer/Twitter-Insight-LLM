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
import urllib.parse

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
        # 每次初始化时重新加载环境变量
        load_dotenv(override=True)
        
        # 从环境变量中获取配置
        self.auth_token = os.getenv('TWITTER_AUTH_TOKEN')  # Twitter 认证令牌
        self.start_date = os.getenv('START_DATE', '2023-01-01')  # 数据抓取的开始日期，默认为2023-01-01
        self.end_date = os.getenv('END_DATE', '2023-01-02')  # 数据抓取的结束日期，默认为2023-01-02
        self.twitter_url = os.getenv('TWITTER_URL', 'https://x.com/elonmusk')  # Twitter 用户页面的 URL
        
        logger.info(f"配置信息: 开始日期={self.start_date}, 结束日期={self.end_date}, URL={self.twitter_url}")
        
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

    def set_token(self, auth_token=None):
        """
        设置 Twitter 认证令牌。
        参数：
            auth_token (str): Twitter 认证令牌。
        """
        if auth_token is None:
            auth_token = self.auth_token
            
        if not auth_token or auth_token == "YOUR_TWITTER_AUTH_TOKEN_HERE":
            raise ValueError("访问令牌缺失。请正确配置它。")
        expiration = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        cookie_script = f"document.cookie = 'auth_token={auth_token}; expires={expiration}; path=/';"
        self.driver.execute_script(cookie_script)

    def fetch_tweets(self, user_url=None, start_date=None, end_date=None):
        """
        抓取指定用户在指定日期范围内的推文数据。
        参数：
            user_url (str): Twitter 用户页面的 URL。
            start_date (str): 数据抓取的开始日期，格式为 YYYY-MM-DD。
            end_date (str): 数据抓取的结束日期，格式为 YYYY-MM-DD。
        """
        # 使用实例变量的默认值
        if user_url is None:
            user_url = self.twitter_url
        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date
            
        # 从URL中提取用户名
        username = user_url.split('/')[-1]
        logger.info(f"准备抓取用户 {username} 的推文")
        
        # 构建高级搜索URL
        search_query = f"(from:{username}) until:{end_date} since:{start_date}"
        encoded_query = urllib.parse.quote(search_query)
        search_url = f"https://x.com/search?q={encoded_query}&src=typed_query&f=live"
        
        logger.info(f"使用搜索URL: {search_url}")
        self.driver.get(search_url)
        
        # 初始化变量用于跟踪作者和日期范围
        author_handles = set()
        min_date = None
        max_date = None
        tweets_data = []

        # 将start_date和end_date从"YYYY-MM-DD"转换为datetime对象
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        logger.info(f"开始抓取日期范围: {start_date} 到 {end_date}")

        # 添加计数器以跟踪处理的推文数量
        processed_count = 0
        max_tweets = 100  # 设置一个上限，防止无限循环
        
        while processed_count < max_tweets:
            try:
                tweet = self._get_first_tweet()
                if not tweet:
                    logger.warning("未找到推文，等待2秒后重试...")
                    time.sleep(2)
                    continue

                row = self._process_tweet(tweet)
                processed_count += 1
                
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
                        logger.warning(f"日期格式值错误: {row['date']}, 错误: {e}")
                        # 如果无法解析日期，跳过此推文
                        self._delete_first_tweet()
                        continue

                    logger.info(f"推文日期: {date.date()}, 开始日期: {start_date_obj.date()}, 结束日期: {end_date_obj.date()}")
                    
                    if date.date() < start_date_obj.date():
                        logger.info(f"推文日期 {date.date()} 早于开始日期 {start_date_obj.date()}，停止抓取")
                        break
                    elif date.date() > end_date_obj.date():
                        logger.info(f"推文日期 {date.date()} 晚于结束日期 {end_date_obj.date()}，跳过此推文")
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
                    f"处理推文 #{processed_count}...\n{row['date']},  {row['author_name']} -- {row['text'][:50]}...\n\n"
                )
                self._delete_first_tweet()
                
                # 每处理10条推文，保存一次数据，防止中途中断丢失数据
                if processed_count % 10 == 0:
                    self._save_intermediate_data(tweets_data, username, start_date, end_date)
                    
            except Exception as e:
                logger.error(f"处理推文时出错: {e}")
                # 继续处理下一条推文
                self._delete_first_tweet()
                continue
        
        # 根据作者handle和日期范围生成文件名
        if len(author_handles) == 1:
            # 单一作者
            prefix = next(iter(author_handles))
        else:
            # 多个作者
            prefix = username
        
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
        
        logger.info(f"已保存 {len(tweets_data)} 条推文到 {cur_filename}.json")
        
        # 保存到Excel
        try:
            self._save_to_excel(
                json_filename=f"{cur_filename}.json", output_filename=f"{cur_filename}.xlsx"
            )
        except Exception as e:
            logger.error(f"保存Excel文件时出错: {e}")
            # 如果保存Excel失败，至少我们有JSON数据

    def _save_intermediate_data(self, tweets_data, username, start_date, end_date):
        """
        保存中间数据，防止中途中断丢失数据。
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"data/{username}_intermediate_{timestamp}.json"
        with open(filename, "w", encoding="utf-8") as file:
            for tweet in tweets_data:
                json.dump(tweet, file)
                file.write("\n")
        logger.info(f"已保存中间数据到 {filename}")

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
                    "遇到 'Something went wrong. Try reloading.' 错误。\n尝试通过切换标签页解决问题。\n"
                )
                logger.info(
                    "不用担心数据重复。保存到Excel时会进行去重。"
                )
                self._navigate_tabs()

                WebDriverWait(self.driver, timeout).until(
                    lambda d: d.find_elements(
                        By.XPATH, "//article[@data-testid='tweet']"
                    )
                )
            elif error_message and not use_hacky_workaround_for_reloading_issue:
                raise TimeoutException(
                    "存在错误信息。未使用解决方案。"
                )

            else:
                # 如果没有错误信息，返回第一条推文
                return self.driver.find_element(
                    By.XPATH, "//article[@data-testid='tweet']"
                )

        except TimeoutException:
            logger.error("等待推文或点击'Retry'后超时")
            raise
        except NoSuchElementException:
            logger.error("找不到推文或'Retry'按钮")
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
            logger.error("切换标签页时出错: " + str(e))

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
            logger.error(f"处理推文时出错: {e}")
            logger.info(f"推文: {tweet}")
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
            logger.info("找不到要删除的第一条推文。")

    @staticmethod
    def _save_to_excel(json_filename, output_filename="data/data.xlsx"):
        """
        将 JSON 数据保存为 Excel 文件。
        参数：
            json_filename (str): JSON 文件路径。
            output_filename (str): 输出的 Excel 文件路径。
        """
        try:
            # 读取 JSON 数据，但不自动解析日期
            cur_df = pd.read_json(json_filename, lines=True, convert_dates=False)
            
            # 手动处理日期时区问题 - Excel不支持带时区的日期时间
            if 'date' in cur_df.columns:
                # 将带时区的日期时间字符串转换为不带时区的datetime对象
                cur_df['date'] = cur_df['date'].apply(
                    lambda x: datetime.fromisoformat(x.replace('Z', '+00:00')).replace(tzinfo=None) 
                    if isinstance(x, str) else x
                )

            # 去重并保存到 Excel
            cur_df.drop_duplicates(subset=["url"], inplace=True)
            cur_df.to_excel(output_filename, index=False)
            logger.info(
                f"\n\n已完成保存到 {output_filename}。共 {len(cur_df)} 条唯一推文。"
            )
        except Exception as e:
            logger.error(f"保存Excel文件时出错: {e}")
            # 打印更详细的错误信息
            import traceback
            logger.error(traceback.format_exc())
            
            # 尝试保存为CSV格式，这通常不会有时区问题
            try:
                csv_output = output_filename.replace('.xlsx', '.csv')
                cur_df.to_csv(csv_output, index=False)
                logger.info(f"已保存为CSV格式: {csv_output}")
            except Exception as csv_e:
                logger.error(f"保存CSV文件也失败: {csv_e}")


if __name__ == "__main__":
    try:
        # 启动 TwitterExtractor 实例
        scraper = TwitterExtractor(headless=False)  # 设置为False以便查看浏览器操作
        # 抓取推文数据
        scraper.fetch_tweets()  # 使用.env中的配置
    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
