# 1683. Invalid Tweets

- ### Intuition
    The goal of this task is to identify invalid tweets based on their length. A tweet is considered **invalid** if the number of characters in the `content` column exceeds 15. The `tweet_id` is the unique identifier for each tweet, and we need to return the IDs of those tweets whose content exceeds the specified character limit.

- ### Approach
    1. **Identify Invalid Tweets**:
        - A tweet is invalid if the length of the `content` column is strictly greater than 15 characters.
        - We need to check the length of the `content` for each tweet and filter those that have more than 15 characters.

    2. **Select the tweet IDs**:
        - Once the invalid tweets are filtered, we need to select only the `tweet_id` of those invalid tweets.
    
    3. **Return the Result**:
        - The output should be a list of `tweet_id`s corresponding to the invalid tweets, which can be returned in any order.

    4. **Result Format**:
        - The result should be a table containing a single column `tweet_id` which holds the IDs of the invalid tweets.

- ### Code Implementation
    - **SQL:**
        ```sql []
        SELECT tweet_id FROM Tweets
        WHERE LENGTH(content) > 15
        ```
    - **PySpark:**
        ```python3 []
        def invalid_tweets(tweets: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
            output = tweets.filter(length(tweets.content) > 15).select('tweet_id')
            return output
        ```
    - **Pandas:**
        ```python3 []
        def invalid_tweets(tweets: pd.DataFrame) -> pd.DataFrame:
            output = tweets[tweets.content.str.len() > 15].tweet_id
            output = pd.DataFrame(output)
            return output
        ```