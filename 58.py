import os
import requests
import time
import random
import json
import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from base64 import b64encode
from tqdm import tqdm
import jieba
from collections import Counter

# --- 配置区 ---
# 请确保你的Cookie是有效的
COOKIE = "_iuqxldmzr_=32; _ntes_nnid=f363969f0e92cb3424804037172d6958,1762855617820; _ntes_nuid=f363969f0e92cb3424804037172d6958; NMTID=00OG-PLxKEu9KuZwktMk4fmi8ehSZQAAAGacmID3g; WEVNSM=1.0.0; WNMCID=jjgybt.1762855619857.01.0; sDeviceId=YD-H2Ey9XXJ%2B4RFAkABFBeCmXsMbRwGN94F; __snaker__id=RgO74WqVsztHGCsh; gdxidpyhxdE=HjwKsAXnXwJJJuO6QebVH7AwL%5CV4mDTdeEHKxMA99Tmf3do8%2F0qJHWuDVaRUHuWGeN7fISWA0lXPJKs%2BDEZSmHsPGmzdZQLluaWp7y8L7srKZlRQ5uvDfPsz%2F3cQiR4L7qHTYSJXtS4j%2BaATXTb0wPkTWfgooWfMwsJEtb9%5CnawnybRt%3A1762973440710; MUSIC_U=00C11CD60E609F40A6CA4785A21B000AB8BD68B9B99261B2CF57B4C2873A43BAFDABABE821057B24317F157A7553337AC316435E8290B1002F4F79D6E850EF7D8D545F4D21C2A3A10C035D2E928647AE1435A636F44DF92A0972ECE929A2872B037CC6C70D2271773BD84C769B7DC967A42776AD4412E986D2FAC8068201805309AD912D643AB7E79BF41AE28B6ACB33254B9E50EBCC52D9CD1FF2FE5B94ADE13CD011A3A5EBDF7485445354A4F584B2007484BB7B70D18F31A47D10A729209D1A1810FF716CD79FE6C519BBFD2C167806E6604B0BCC5EBBEB7167BCD1B6D0E9625DC336A91275EB93FB2A23D5F52991DE4D532D0DBBAEED9E9491AE81E44FC4114ABB4B5B939A7B88DB9499EFC03267FE2AF23E9DD050CB66E31A606EAEDC95EA762AF0BAEC2EB0BEF2BDEE1070CC0B5EE160759364FE03A297E87B6C042EA19F9924E06E92AE549DD4BF2F785BC3B163CE9CBBF6D7F1DDA2E12A6DF814A2039B5FF89990C3F493FDBDE2B7A374188D0F34509B413CA7275CAB1A1CF06F9890059DF6533F699FC9CAD4E5427CBADA030C9908137EBFD896628811B6F4D9C9738A; __remember_me=true; __csrf=90dea7b7c5ecdef24a3e25a4420c97ec; ntes_kaola_ad=1; WM_NI=b%2Bc4fTqOA5HA7ygqsbHDid4otf%2B58vyuYIZ5OMnaIlPmUiz4fpEtxxAdkRBEHfYdwhs7otdD%2BO7bWYIaN9KZVosP96pYvlIjUL4HoekWy2Gext%2BP5KijTK8jpN1oyLzySU4%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6ee86d374b28aafa2d54df3e78fb3c15e878e8e87c76798aa9cd2bb3c88a89793b42af0fea7c3b92aa2f5add5e560b3918fd6cb4bf5a7a992f26a8c9ca785b765b8bae5d6bb7ebbab97aad93f8d93a282cc6894b39b8af940a6b08b95fb7483bca5b3c14f8cba9785f664b1e9a195e750e991f983d6538bbf82daca5296b1bdb1bc3eb487ba8dcc629794fca9ee6fa989b7d3c66fbcace5b1b841abb69cacbb62b492ff90fc48baa79db6b737e2a3; WM_TID=3UBogJWkYx1FAQFBVFOSzD%2FHw9lX0Mqm; ntes_utid=tid._.e98C3nNTm45FAwEUAEbGzCuWwphXz1E1._.0; JSESSIONID-WYYY=k1B4iMSKqXTccaQS7WHPKfZG5%2F75%5CG8uKzO81pmqT6QsNQlsK6nsyKqic%2BADm1865HnpTIHKE6M8aX4tE%5Cx5DNkKnOziGk3GwRkqXOZwVeTPza0XDyeugWisYHyF5t0cx88%2Bp8lkjgDpQo%2FbsUk6FSUgJB%2FNbNyb3hmzgwaF5S5b62Ap%3A1763030213253"

# 要抓取的歌单ID和名称
PLAYLISTS = [
    {"id": "3778678", "name": "热歌榜"},
    {"id": "3779629", "name": "新歌榜"},
    {"id": "19723756", "name": "飙升榜"},
    {"id": "2884035", "name": "原创榜"},
]

# 每首歌抓取的评论数（每次请求的数量）
COMMENTS_PER_REQUEST = 20
# 目标抓取的总评论数（点赞前 N 条）
TARGET_COMMENTS_COUNT = 100
# 爬取延迟（秒）
REQUEST_DELAY = 4
# 结果存储根目录
ROOT_RESULT_DIR = "multi_playlist_results"
# 情感分析阈值（可调整）
NEGATIVE_THRESHOLD = 0.4  # 消极阈值
POSITIVE_THRESHOLD = 0.6  # 积极阈值

# --- 加密函数 (无需修改) ---
def encrypt_aes(text, key, iv):
    """使用AES-CBC模式加密文本"""
    cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
    padded_text = pad(text.encode("utf-8"), AES.block_size)
    ciphertext = cipher.encrypt(padded_text)
    return b64encode(ciphertext).decode("utf-8")

def b(a, b_key):
    """构造加密请求体"""
    c = b_key
    d = "0102030405060708"
    e = a
    f = encrypt_aes(e, c, d)
    return f

def extract_high_freq_words(comments, top_n=5):
    """
    从评论列表中提取高频词（优化版）。
    """
    if not comments:
        return ""

    # 1. 合并所有评论为一个长文本
    combined_text = " ".join(comments)

    # 2. 使用自定义词典来纠正分词错误
    custom_dict = [
        "李宇春", "玉米糊", "玉米油",
        # ... 你可以根据实际情况添加更多
    ]
    for word in custom_dict:
        jieba.add_word(word)

    # 3. 使用jieba进行分词
    words = jieba.cut(combined_text)

    # 4. 使用更全面的停用词表
    stop_words = {
        '，', '。', '、', '；', '：', '？', '！', '"', '"', ''', ''',
        '（', '）', '【', '】', '《', '》', '…', '—', '-', ' ', '\n', '\t',
        '的', '了', '是', '我', '在', '和', '也', '都', '很', '就', '还',
        '有', '这个', '那个', '这里', '那里', '什么', '怎么', '哪里', '为什么',
        '你', '我', '他', '她', '它', '我们', '你们', '他们', '这', '那', '上',
        '下', '不', '人', '一', '一个', '到', '着', '去', '来', '要', '会', '让',
        '叫', '说', '想', '看', '听', '觉得', '知道', '可以', '可能', '应该',
        '哈哈', '哈哈哈', '呵呵', '嘿嘿', '嘻嘻', '呃', '嗯', '啊', '哦', '噢',
        '呜', '嘛', '呢', '吧', '哒', '呀', '耶', '哟', '啊哈', '天呐', '卧槽',
        '玉米'
    }

    word_counts = Counter()

    for word in words:
        # 5. 过滤条件：不在停用词表中、长度大于1、不是纯数字
        if word not in stop_words and len(word) > 1 and not word.isdigit():
            word_counts[word] += 1

    # 6. 获取前N个高频词
    top_words = [word for word, count in word_counts.most_common(top_n)]

    return ",".join(top_words)

# --- 核心功能函数 ---
def get_playlist_tracks(playlist_id, playlist_name):
    """
    获取歌单中的所有歌曲信息。
    步骤1: 获取歌单元数据，提取所有歌曲的ID (trackIds)。
    步骤2: 使用歌曲ID批量获取每首歌的详细信息。
    """
    if not playlist_id or playlist_id.strip() == "":
        print(f"歌单 '{playlist_name}' ID为空，跳过")
        return []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Cookie": COOKIE,
        "Referer": f"https://music.163.com/playlist?id={playlist_id.strip()}",
    }

    all_songs = []
    track_ids = []

    print(f"  - [步骤1/2] 获取歌单 '{playlist_name}' 的歌曲ID列表...")
    # --- 步骤1: 获取 trackIds 列表 ---
    try:
        playlist_url = f"https://music.163.com/api/v6/playlist/detail?id={playlist_id.strip()}"
        response = requests.get(playlist_url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get('code') != 200:
            print(f"  - 获取歌单元数据失败: {data.get('message', '未知错误')}")
            return []

        playlist_data = data.get('playlist', {})
        # 从歌单数据中提取所有歌曲的ID
        track_ids = [item.get('id') for item in playlist_data.get('trackIds', []) if item.get('id')]

        if not track_ids:
            print(f"  - 警告: 未在歌单中找到任何歌曲ID (trackIds)。")
            # 降级处理：尝试从 tracks 字段获取
            tracks = playlist_data.get('tracks', [])
            track_ids = [track.get('id') for track in tracks if track.get('id')]
            print(f"  - 降级尝试从 tracks 获取到 {len(track_ids)} 首歌曲ID。")

        print(f"  - 成功获取到 {len(track_ids)} 首歌曲的ID。")

    except requests.exceptions.RequestException as e:
        print(f"  - 获取歌单元数据时发生网络错误: {e}")
        return []

    if not track_ids:
        return []

    # --- 步骤2: 根据 trackIds 批量获取歌曲详情 ---
    print(f"  - [步骤2/2] 根据ID批量获取歌曲详情...")
    song_detail_url = "https://music.163.com/api/song/detail"
    batch_size = 100  # 每次请求100首，这是API的限制

    for i in range(0, len(track_ids), batch_size):
        batch_ids = track_ids[i:i + batch_size]
        params = {
            "ids": json.dumps(batch_ids)
        }
        try:
            response = requests.get(song_detail_url, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            detail_data = response.json()

            if detail_data.get('code') != 200:
                print(f"  - 获取歌曲详情批次 {i//batch_size + 1} 失败: {detail_data.get('message', '未知错误')}")
                continue

            songs = detail_data.get('songs', [])
            for song in songs:
                song_id = song.get('id')
                song_name = song.get('name', '未知歌曲')
                artists = '/'.join([art.get('name', '未知歌手') for art in song.get('artists', [])])
                all_songs.append({
                    'id': song_id,
                    'name': song_name,
                    'artists': artists
                })
            print(f"  - 成功获取批次 {i//batch_size + 1}/{(len(track_ids) + batch_size - 1) // batch_size}，新增 {len(songs)} 首歌曲信息。")

        except requests.exceptions.RequestException as e:
            print(f"  - 获取歌曲详情批次 {i//batch_size + 1} 时发生网络错误: {e}")

    print(f"  - 完成歌曲信息获取，共收集到 {len(all_songs)} 首有效歌曲。")
    return all_songs

def fetch_comments_detailed(song_id):
    """
    抓取指定歌曲点赞量前 TARGET_COMMENTS_COUNT 条的评论，并进行情感分析和高频词提取。
    """
    csrf_token = ""
    if '__csrf=' in COOKIE:
        csrf_token = COOKIE.split('__csrf=')[1].split(';')[0].strip()
    if not csrf_token:
        print(f"  - 歌曲 ID {song_id}：未获取到csrf_token，评论请求失败")
        return pd.DataFrame(), {}

    all_comments_list = []  # 用于存储所有抓取到的评论字典
    offset = 0
    max_retries = 3  # 最大重试次数

    print(f"    - 开始抓取点赞前 {TARGET_COMMENTS_COUNT} 条评论...")

    while len(all_comments_list) < TARGET_COMMENTS_COUNT:
        d = {
            "csrf_token": csrf_token,
            "cursor": "-1",
            "offset": str(offset),
            "orderType": "2",  # 2 表示按点赞数降序排列
            "pageNo": "1",
            "pageSize": str(COMMENTS_PER_REQUEST),
            "rid": f"R_SO_4_{song_id}",
            "threadId": f"R_SO_4_{song_id}",
        }

        i = "BdQMOhNkLlEP6jc7"
        g = "0CoJUm6Qyw8W8jud"
        d_json = json.dumps(d, ensure_ascii=False)
        enc_text = b(d_json, g)
        enc_text = b(enc_text, i)
        enc_sec_key = "1cac8643f7b59dbd626afa11238b1a90fab1e08bc8dabeec8b649e8a121b63fc45c2bc3427c6a9c6e6993624ec2987a2547c294e73913142444ddeec052b6ec2f9a4bebf57784d250e08749f371d94b635159a1c6ebfda81ee40600f2a22a5c1e7f0903884e4b466024a8905f0074a9432fd79c24ccf6aff73ea36fd68153031"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Cookie": COOKIE,
            "Referer": f"https://music.163.com/song?id={song_id}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://music.163.com"
        }
        url = "https://music.163.com/weapi/comment/resource/comments/get?csrf_token=" + csrf_token
        data = {"params": enc_text, "encSecKey": enc_sec_key}

        try:
            response = requests.post(url, headers=headers, data=data, timeout=15)
            response.raise_for_status()
            respond_data = response.json()

            if respond_data.get('code') != 200:
                print(f"    - 歌曲 ID {song_id} 评论请求失败：{respond_data.get('message', '未知错误')}")
                break

            comments_data = respond_data.get("data", {}).get("comments", [])
            if not comments_data:
                print(f"    - 没有更多评论了。")
                break

            # 处理本次请求获取的评论
            for item in comments_data:
                user = item.get('user', {})
                comment_time = item.get("time", 0)
                formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(str(comment_time)[:10]))) if comment_time else "未知时间"

                comment_dict = {
                    "user_name": user.get("nickname", "匿名用户").replace(",", "，"),
                    "user_city": item.get('ipLocation', {}).get("location", "未知"),
                    "comment": item.get("content", "").strip().replace("\n", "").replace(",", "，"),
                    "praise": str(item.get("likedCount", 0)),
                    "date": formatted_time
                }
                all_comments_list.append(comment_dict)

            print(f"    - 已抓取 {len(all_comments_list)} 条评论...")

            # 如果本次获取的评论数少于请求数，说明已经到底了
            if len(comments_data) < COMMENTS_PER_REQUEST:
                break

            offset += COMMENTS_PER_REQUEST
            time.sleep(random.uniform(REQUEST_DELAY * 0.5, REQUEST_DELAY * 1)) # 每次请求后延迟

        except requests.exceptions.RequestException as e:
            print(f"    - 歌曲 ID {song_id} 评论请求时发生网络错误: {e}")
            max_retries -= 1
            if max_retries <= 0:
                print(f"    - 重试次数耗尽，放弃抓取。")
                break
            time.sleep(5) # 出错后延迟更长时间重试

    # 截取前 TARGET_COMMENTS_COUNT 条评论
    final_comments_list = all_comments_list[:TARGET_COMMENTS_COUNT]
    
    if not final_comments_list:
        print(f"  - 歌曲 ID {song_id} 未获取到有效评论")
        return pd.DataFrame(), {}

    df = pd.DataFrame(final_comments_list)

    # 情感分析
    from snownlp import SnowNLP
    df["sentiment"] = df["comment"].apply(
        lambda x: SnowNLP(x).sentiments if isinstance(x, str) and x.strip() else 0.5
    )

    # 提取高频词（基于最终的评论列表）
    comment_texts = df["comment"].tolist()
    high_freq_words = extract_high_freq_words(comment_texts, top_n=5)

    # 统计
    total = len(df)
    positive_count = len(df[df["sentiment"] >= POSITIVE_THRESHOLD])
    negative_count = len(df[df["sentiment"] <= NEGATIVE_THRESHOLD])
    neutral_count = total - positive_count - negative_count

    summary = {
        'total_comments': total,
        'positive_count': positive_count,
        'negative_count': negative_count,
        'neutral_count': neutral_count,
        'positive_ratio': round(positive_count / total if total > 0 else 0, 4),
        'negative_ratio': round(negative_count / total if total > 0 else 0, 4),
        'neutral_ratio': round(neutral_count / total if total > 0 else 0, 4),
        'high_freq_words': high_freq_words
    }

    print(f"  - 成功抓取 {summary['total_comments']} 条评论用于分析。")
    return df, summary

# --- 保存数据函数 ---
def save_song_data_for_playlist(song_info, comments_df, summary, playlist_name):
    """
    将单首歌曲的评论数据和统计信息（含中立和高频词）保存到文件。
    """
    playlist_dir = os.path.join(ROOT_RESULT_DIR, playlist_name)
    detailed_comment_dir = os.path.join(playlist_dir, "detailed_comments")
    dataset_csv_path = os.path.join(playlist_dir, f"{playlist_name}_dataset.csv")

    os.makedirs(detailed_comment_dir, exist_ok=True)

    # 构建行数据
    dataset_row = {
        '歌曲ID': song_info['id'],
        '歌曲名称': song_info['name'],
        '歌手': song_info['artists'],
        '抓取时间': time.strftime("%Y-%m-%d %H:%M:%S"),
        '评论总数': summary.get('total_comments', 0),
        '积极评论数': summary.get('positive_count', 0),
        '消极评论数': summary.get('negative_count', 0),
        '中立评论数': summary.get('neutral_count', 0),
        '积极评论占比': summary.get('positive_ratio', 0),
        '消极评论占比': summary.get('negative_ratio', 0),
        '中立评论占比': summary.get('neutral_ratio', 0),
        '高频字眼': summary.get('high_freq_words', '')
    }

    # 保存汇总数据
    if not os.path.exists(dataset_csv_path):
        pd.DataFrame([dataset_row]).to_csv(dataset_csv_path, index=False, encoding='utf-8-sig')
    else:
        pd.DataFrame([dataset_row]).to_csv(dataset_csv_path, mode='a', header=False, index=False, encoding='utf-8-sig')

    # 保存详细评论
    if not comments_df.empty:
        comments_df.rename(columns={
            'user_name': '用户名',
            'user_city': '用户城市',
            'comment': '评论内容',
            'praise': '点赞数',
            'date': '评论时间',
            'sentiment': '情感得分'
        }, inplace=True)

        comment_csv_path = os.path.join(detailed_comment_dir, f"comments_{song_info['id']}.csv")
        comments_df.to_csv(comment_csv_path, index=False, encoding='utf-8-sig')

# --- 主函数 ---
def main():
    """爬虫主入口函数"""
    os.makedirs(ROOT_RESULT_DIR, exist_ok=True)

    for playlist in PLAYLISTS:
        playlist_id = playlist["id"]
        playlist_name = playlist["name"]
        
        # 在处理每个歌单前，检查并删除已存在的数据集文件，以避免重复数据
        dataset_csv_path = os.path.join(ROOT_RESULT_DIR, playlist_name, f"{playlist_name}_dataset.csv")
        if os.path.exists(dataset_csv_path):
            try:
                os.remove(dataset_csv_path)
                print(f"\n已删除旧的数据集文件: {dataset_csv_path}")
            except OSError as e:
                print(f"\n删除旧数据集文件时出错: {e}")

        print(f"\n{'=' * 20} 开始处理榜单: {playlist_name} (ID: {playlist_id}) {'=' * 20}")

        song_list = get_playlist_tracks(playlist_id, playlist_name)
        if not song_list:
            print(f"跳过榜单: {playlist_name}，未获取到歌曲。")
            continue

        print(f"准备处理 {len(song_list)} 首歌曲的评论...")
        # 使用tqdm进度条遍历歌曲列表
        for song_info in tqdm(song_list, desc=f"处理 {playlist_name} 的歌曲"):
            print(f"\n正在处理歌曲: {song_info['name']} - {song_info['artists']} (ID: {song_info['id']})")
            comments_df, summary = fetch_comments_detailed(song_info['id'])
            if not comments_df.empty and summary:
                save_song_data_for_playlist(song_info, comments_df, summary, playlist_name)
                print(f"  - 成功保存 {summary['total_comments']} 条评论。高频词：{summary['high_freq_words']}")
            else:
                print(f"  - 未获取到有效评论或保存失败。")

            # 随机延迟，模拟人类行为
            time.sleep(random.uniform(REQUEST_DELAY * 0.8, REQUEST_DELAY * 1.5))

        print(f"\n{'=' * 20} 榜单 '{playlist_name}' 处理完毕 {'=' * 20}")

    print(f"\n所有榜单处理完成！结果保存在 '{ROOT_RESULT_DIR}' 目录下。")

if __name__ == "__main__":
    main()