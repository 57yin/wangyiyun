import os
import shutil
import requests
import time
import random
import json
import pandas as pd
import jieba
from collections import Counter
import schedule
from tqdm import tqdm

# --- 配置区 ---
COOKIE = "_iuqxldmzr_=32; _ntes_nnid=f363969f0e92cb3424804037172d6958,1762855617820; _ntes_nuid=f363969f0e92cb3424804037172d6958; NMTID=00OG-PLxKEu9KuZwktMk4fmi8ehSZQAAAGacmID3g; WEVNSM=1.0.0; WNMCID=jjgybt.1762855619857.01.0; sDeviceId=YD-H2Ey9XXJ%2B4RFAkABFBeCmXsMbRwGN94F; __snaker__id=RgO74WqVsztHGCsh; gdxidpyhxdE=HjwKsAXnXwJJJuO6QebVH7AwL%5CV4mDTdeEHKxMA99Tmf3do8%2F0qJHWuDVaRUHuWGeN7fISWA0lXPJKs%2BDEZSmHsPGmzdZQLluaWp7y8L7srKZlRQ5uvDfPsz%2F3cQiR4L7qHTYSJXtS4j%2BaATXTb0wPkTWfgooWfMwsJEtb9%5CnawnybRt%3A1762973440710; MUSIC_U=00C11CD60E609F40A6CA4785A21B000AB8BD68B9B99261B2CF57B4C2873A43BAFDABABE821057B24317F157A7553337AC316435E8290B1002F4F79D6E850EF7D8D545F4D21C2A3A10C035D2E928647AE1435A636F44DF92A0972ECE929A2872B037CC6C70D2271773BD84C769B7DC967A42776AD4412E986D2FAC8068201805309AD912D643AB7E79BF41AE28B6ACB33254B9E50EBCC52D9CD1FF2FE5B94ADE13CD011A3A5EBDF7485445354A4F584B2007484BB7B70D18F31A47D10A729209D1A1810FF716CD79FE6C519BBFD2C167806E6604B0BCC5EBBEB7167BCD1B6D0E9625DC336A91275EB93FB2A23D5F52991DE4D532D0DBBAEED9E9491AE81E44FC4114ABB4B5B939A7B88DB9499EFC03267FE2AF23E9DD050CB66E31A606EAEDC95EA762AF0BAEC2EB0BEF2BDEE1070CC0B5EE160759364FE03A297E87B6C042EA19F9924E06E92AE549DD4BF2F785BC3B163CE9CBBF6D7F1DDA2E12A6DF814A2039B5FF89990C3F493FDBDE2B7A374188D0F34509B413CA7275CAB1A1CF06F9890059DF6533F699FC9CAD4E5427CBADA030C9908137EBFD896628811B6F4D9C9738A; __remember_me=true; __csrf=90dea7b7c5ecdef24a3e25a4420c97ec; ntes_kaola_ad=1; WM_NI=VePaWKbn%2FyIr8r1%2FNusDOjwuk0ZTKX25EjT%2B9lusitRVO93W%2BQCAOt72dAI20QfpftzawYNSnGQXG2wyrlNt95mORQ45UPXjh1ExL80Gq%2FctGZ%2FNlDen8j2SVXSH78z3NGg%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6eed7f25089b59e95d97fbb868fa3d45f969f8a83d73aa89c0083f57eb293b98fcc2af0fea7c3b92a96bba082d641a2b2e19acc7afcaeffd0cd7fb19c818bf67fa9f096b5ca54f2eb8b8bd721fbb4bb94ec80928a81dab25de98f8da2ce80f5eeaca2cc3af48faa91e580babfb9bacd59ae999fb9c63ab5a7be8dcd80fbeba3b0d15bf8eb9bb1db62acef8b99cf34f499bfd0e2808586e1afef50a7b2f88bb15996889da9b634baed82a8e637e2a3; WM_TID=X3yumqhL5NpEQBERFReH2IHwf7%2Fud0NQ; ntes_utid=tid._.MKIqFQdCdKFBVwRABQPS2IThO7q7OpWQ._.0; JSESSIONID-WYYY=%2F9W19tCZuhR%5Cu1nJcZsXEw%2FvjgC30XGlxJ0Yldfy9pp9P4aWEX%5C4w3xQ3QsTAqcEIBu4IYPZ1DKNNvgXudzFn7e98bovDhhyOTu2IQM9O6EXWFAzpUHmqiimo8mI%5CD53YWz69sDg49Vm21EX7lu5%2F93rPGQeEtdbAJ9Gr41OZSzu59dc%3A1763302547466"
PLAYLISTS = [
    {"id": "3778678", "name": "热歌榜"},
    {"id": "3779629", "name": "新歌榜"},
    {"id": "19723756", "name": "飙升榜"},
    {"id": "2884035", "name": "原创榜"},
]

COMMENTS_PER_REQUEST = 20  # 每次请求获取20条
REQUEST_DELAY = 4  # 歌曲之间的请求间隔（秒）
PAGE_REQUEST_DELAY = 2  # 每页评论之间的请求间隔（秒），增加此值以降低频率
ROOT_RESULT_DIR = r"D:\vscode\analysis_app\multi_playlist_results"
NEGATIVE_THRESHOLD = 0.4  # 消极情感阈值
POSITIVE_THRESHOLD = 0.6  # 积极情感阈值
DAILY_UPDATE_TIME = "02:00"  # 定时任务时间

# 核心配置：普通评论采集比例（总评论数的 0.0005）
TARGET_NORMAL_COMMENT_RATIO = 0.0005

# --- 常量定义（加密/API相关，部分保留用于兼容扩展）---
AES_KEY = "0CoJUm6Qyw8W8jud"
AES_IV = "0102030405060708"
SECOND_AES_KEY = "BdQMOhNkLlEP6jc7"
ENC_SEC_KEY = "1cac8643f7b59dbd626afa11238b1a90fab1e08bc8dabeec8b649e8a121b63fc45c2bc3427c6a9c6e6993624ec2987a2547c294e73913142444ddeec052b6ec2f9a4bebf57784d250e08749f371d94b635159a1c6ebfda81ee40600f2a22a5c1e7f0903884e4b466024a8905f0074a9432fd79c24ccf6aff73ea36fd68153031"
COMMENT_API_URL = "https://music.163.com/weapi/comment/resource/comments/get"

# --- 文本处理函数 ---
def extract_high_freq_words(comments, top_n=5):
    """提取评论高频词（过滤停用词）"""
    if not comments:
        return ""
    combined_text = " ".join(comments)
    # 自定义词典（可根据需求扩展）
    custom_dict = ["李宇春", "玉米糊", "玉米油"]
    for word in custom_dict:
        jieba.add_word(word)
    # 分词+过滤停用词
    words = jieba.cut(combined_text)
    stop_words = {
        '，', '。', '、', '；', '：', '？', '！', '"', '（', '）', '【', '】', '《', '》', '…', '—', '-', ' ', '\n', '\t',
        '的', '了', '是', '我', '在', '和', '也', '都', '很', '就', '还', '有', '这个', '那个', '这里', '那里', '什么', '怎么', '哪里', '为什么',
        '你', '他', '她', '它', '我们', '你们', '他们', '这', '那', '上', '下', '不', '人', '一', '一个', '到', '着', '去', '来', '要', '会', '让',
        '叫', '说', '想', '看', '听', '觉得', '知道', '可以', '可能', '应该', '哈哈', '哈哈哈', '呵呵', '嘿嘿', '嘻嘻', '呃', '嗯', '啊', '哦', '噢',
        '呜', '嘛', '呢', '吧', '哒', '呀', '耶', '哟', '啊哈', '天呐', '玉米', '首歌'
    }
    # 统计词频
    word_counts = Counter()
    for word in words:
        if word not in stop_words and len(word) > 1 and not word.isdigit():
            word_counts[word] += 1
    # 返回TOP N高频词
    return ",".join([word for word, count in word_counts.most_common(top_n)])

# --- 新增函数：获取歌曲总评论数 ---
def get_total_comments_count(song_id, headers):
    """获取歌曲总评论数"""
    song_id = str(song_id).strip()
    try:
        # 请求第一页获取总评论数（仅请求1条数据，提高效率）
        test_url = f'http://music.163.com/api/v1/resource/comments/R_SO_4_{song_id}?limit=1&offset=0'
        response = requests.get(test_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get('code') == 200:
            return data.get('total', 0)  # 网易云API返回的总评论数字段为total
        return 0
    except Exception as e:
        print(f"    - 获取总评论数失败: {str(e)}")
        return 0

# --- 核心功能函数 ---
def get_playlist_tracks(playlist_id, playlist_name):
    """获取歌单中的所有歌曲信息（ID、名称、歌手）"""
    playlist_id = str(playlist_id).strip()
    if not playlist_id.isdigit():
        print(f"歌单 '{playlist_name}' ID格式错误（需纯数字），跳过")
        return []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Cookie": COOKIE,
        "Referer": f"https://music.163.com/playlist?id={playlist_id}",
    }
    all_songs, track_ids = [], []
    print(f"  - [步骤1/2] 获取歌单 '{playlist_name}' 的歌曲ID列表...")

    try:
        # 请求歌单详情（获取trackIds）
        playlist_url = f"https://music.163.com/api/v6/playlist/detail?id={playlist_id}"
        response = requests.get(playlist_url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get('code') != 200:
            print(f"  - 获取歌单元数据失败: {data.get('message', '未知错误')}")
            return []

        # 提取歌曲ID（优先从trackIds获取，失败则降级从tracks获取）
        playlist_data = data.get('playlist', {})
        track_ids = [str(item.get('id')).strip() for item in playlist_data.get('trackIds', []) if item.get('id') and str(item.get('id')).strip().isdigit()]
        if not track_ids:
            print(f"  - 警告: 未在歌单中找到有效歌曲ID (trackIds)，尝试降级获取...")
            tracks = playlist_data.get('tracks', [])
            track_ids = [str(track.get('id')).strip() for track in tracks if track.get('id') and str(track.get('id')).strip().isdigit()]
            print(f"  - 降级成功，获取到 {len(track_ids)} 首有效歌曲ID。")
        print(f"  - 成功获取到 {len(track_ids)} 首歌曲的ID。")

    except requests.exceptions.RequestException as e:
        print(f"  - 获取歌单元数据时发生网络错误: {str(e)}")
        return []

    if not track_ids:
        return []

    # 批量获取歌曲详情（每批100首，避免请求过多）
    print(f"  - [步骤2/2] 根据ID批量获取歌曲详情...")
    song_detail_url = "https://music.163.com/api/song/detail"
    batch_size = 100
    for i in range(0, len(track_ids), batch_size):
        batch_ids = track_ids[i:i + batch_size]
        params = {"ids": json.dumps([int(id_str) for id_str in batch_ids])}
        try:
            response = requests.get(song_detail_url, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            detail_data = response.json()

            if detail_data.get('code') != 200:
                print(f"  - 获取歌曲详情批次 {i//batch_size + 1} 失败: {detail_data.get('message', '未知错误')}")
                continue

            # 提取歌曲信息（名称、歌手）
            songs = detail_data.get('songs', [])
            for song in songs:
                song_id = str(song.get('id')).strip()
                song_name = song.get('name', '未知歌曲').strip()
                artists = '/'.join([art.get('name', '未知歌手').strip() for art in song.get('artists', [])])
                all_songs.append({
                    'id': song_id,
                    'name': song_name,
                    'artists': artists
                })
            print(f"  - 成功获取批次 {i//batch_size + 1}/{(len(track_ids) + batch_size - 1) // batch_size}，新增 {len(songs)} 首歌曲信息。")

        except requests.exceptions.RequestException as e:
            print(f"  - 获取歌曲详情批次 {i//batch_size + 1} 时发生网络错误: {str(e)}")

    print(f"  - 完成歌曲信息获取，共收集到 {len(all_songs)} 首有效歌曲。")
    return all_songs

def fetch_comments_detailed(song_id, current_playlist_name):
    """
    核心函数：抓取高赞评论 + 按总评论数比例采集普通评论（纯百分比，不截断）
    优化：增加重试机制和更长的请求间隔，以对抗反爬虫。
    返回：评论数据DataFrame + 统计摘要
    """
    song_id = str(song_id).strip()
    if not song_id.isdigit():
        print(f"  - 歌曲 ID {song_id} 格式错误，评论请求失败")
        return pd.DataFrame(), {}

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Cookie": COOKIE,
        "Referer": f"https://music.163.com/song?id={song_id}",
    }

    all_raw_comments = []  # 存储所有抓取到的原始评论（未格式化）

    def _format_comment(comment):
        """内部函数：格式化单条评论"""
        user = comment.get('user', {})
        comment_time = comment.get("time", 0)
        try:
            formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(str(comment_time)[:10])))
        except (ValueError, TypeError):
            formatted_time = "未知时间"

        praise_count = comment.get('likedCount', 0)
        try:
            praise_count = int(praise_count)
        except (ValueError, TypeError):
            praise_count = 0

        comment_content = comment.get("content", "").strip().replace("\n", " ").replace(",", "，")

        return {
            "commentId": comment.get('commentId', ''),  # 使用commentId作为唯一标识
            "user_name": user.get("nickname", "匿名用户").replace(",", "，").strip(),
            "user_city": comment.get('ipLocation', {}).get("location", "未知").strip(),
            "comment": comment_content,
            "praise": praise_count,
            "date": formatted_time
        }

    try:
        # 步骤1：获取总评论数，计算目标普通评论数
        total_comment_count = get_total_comments_count(song_id, headers)

        if current_playlist_name == "新歌榜":
            current_ratio = 0.005
        else:
            current_ratio = TARGET_NORMAL_COMMENT_RATIO

        if total_comment_count <= 0:
            print(f"    - 未获取到歌曲总评论数，按默认逻辑抓取（最多4页普通评论）")
            target_normal_comments = max(25, COMMENTS_PER_REQUEST * 4)
        else:
            target_normal_comments = int(total_comment_count * current_ratio)
            target_normal_comments = max(25, target_normal_comments)
        print(f"    - 歌曲总评论数: {total_comment_count}，目标获取普通评论数: {target_normal_comments}（歌单：{current_playlist_name}，比例：{current_ratio}）")

        # 步骤2：抓取热评（第一页）
        hot_comments = []
        page_url = f'http://music.163.com/api/v1/resource/comments/R_SO_4_{song_id}?limit={COMMENTS_PER_REQUEST}&offset=0'
        try:
            response = requests.get(page_url, headers=headers, timeout=10)
            response.raise_for_status()
            page_data = response.json()

            if page_data.get('code') == 200:
                hot_comments = page_data.get('hotComments', [])
                all_raw_comments.extend(hot_comments)
                print(f"    - 热评页: 获取 {len(hot_comments)} 条热评")
            else:
                print(f"    - 热评页请求失败，错误码: {page_data.get('code')}")
        except Exception as e:
            print(f"    - 热评页请求异常: {str(e)}")

        # 步骤3：抓取普通评论（优化部分）
        collected_normal_comments = 0
        page = 1  # 从第2页开始
        max_retries = 3  # 每页最多重试次数
        max_pages = 100  # 最大页数限制

        while collected_normal_comments < target_normal_comments and page <= max_pages:
            offset = page * COMMENTS_PER_REQUEST
            current_page_limit = min(COMMENTS_PER_REQUEST, target_normal_comments - collected_normal_comments)
            page_url = f'http://music.163.com/api/v1/resource/comments/R_SO_4_{song_id}?limit={current_page_limit}&offset={offset}'

            normal_comments = []
            for attempt in range(max_retries):
                try:
                    response = requests.get(page_url, headers=headers, timeout=15)  # 增加超时时间
                    response.raise_for_status()
                    page_data = response.json()

                    if page_data.get('code') != 200:
                        print(f"    - 普通评论第{page + 1}页请求失败，错误码: {page_data.get('code')}，尝试 {attempt + 1}/{max_retries}")
                        time.sleep(PAGE_REQUEST_DELAY * (attempt + 1))  # 指数退避
                        continue

                    normal_comments = page_data.get('comments', [])
                    if not normal_comments:
                        print(f"    - 普通评论第{page + 1}页无数据，尝试 {attempt + 1}/{max_retries}")
                        time.sleep(PAGE_REQUEST_DELAY * (attempt + 1))  # 等待后重试
                        continue

                    # 如果成功获取数据，则跳出重试循环
                    break
                except Exception as e:
                    print(f"    - 普通评论第{page + 1}页请求异常: {str(e)}，尝试 {attempt + 1}/{max_retries}")
                    time.sleep(PAGE_REQUEST_DELAY * (attempt + 1))

            # 重试结束后检查结果
            if not normal_comments:
                print(f"    - 普通评论第{page + 1}页多次重试后仍无数据，停止抓取该歌曲评论。")
                break  # 多次重试失败，认为是真的没有数据了

            # 成功获取到数据
            all_raw_comments.extend(normal_comments)
            collected_normal_comments += len(normal_comments)
            print(f"    - 普通评论第{page + 1}页: 获取 {len(normal_comments)} 条 (累计: {collected_normal_comments}/{target_normal_comments})")

            # 增加页间延迟，模拟人类行为
            time.sleep(random.uniform(PAGE_REQUEST_DELAY * 0.8, PAGE_REQUEST_DELAY * 1.2))
            page += 1

        # 循环结束后的日志
        if collected_normal_comments >= target_normal_comments:
            print(f"    - 普通评论抓取完成，成功达到目标数量。")
        elif page > max_pages:
            print(f"    - 普通评论抓取达到最大页数限制 ({max_pages}页)，未达到目标数量。")
        # else:  # 因无数据而退出
        #     print(f"    - 普通评论抓取提前结束，未达到目标数量。")

        # 去重和格式化
        unique_comments_dict = {c.get('commentId'): c for c in all_raw_comments if c.get('commentId')}
        final_comments = sorted([_format_comment(c) for c in unique_comments_dict.values()], key=lambda x: x["praise"], reverse=True)

        if not final_comments:
            print(f"  - 歌曲 ID {song_id} 未获取到任何有效评论")
            return pd.DataFrame(), {}

        # 情感分析和统计
        comments_df = pd.DataFrame(final_comments)
        from snownlp import SnowNLP

        comments_df["sentiment"] = comments_df["comment"].apply(
            lambda x: SnowNLP(x).sentiments if isinstance(x, str) and x.strip() else 0.5
        )

        high_freq_words = extract_high_freq_words(comments_df["comment"].tolist(), top_n=5)
        total_valid = len(comments_df)
        positive_count = len(comments_df[comments_df["sentiment"] >= POSITIVE_THRESHOLD])
        negative_count = len(comments_df[comments_df["sentiment"] <= NEGATIVE_THRESHOLD])
        neutral_count = total_valid - positive_count - negative_count

        max_praise = int(comments_df['praise'].max()) if total_valid > 0 else 0
        avg_praise = round(comments_df['praise'].mean(), 1) if total_valid > 0 else 0
        top_comment = (final_comments[0]['comment'][:50] + "...") if final_comments[0]['comment'] else ""
        top_praise = final_comments[0]['praise'] if final_comments else 0

        summary = {
            'total_comments': total_valid,
            'positive_count': positive_count,
            'negative_count': negative_count,
            'neutral_count': neutral_count,
            'positive_ratio': round(positive_count / total_valid if total_valid > 0 else 0, 4),
            'negative_ratio': round(negative_count / total_valid if total_valid > 0 else 0, 4),
            'neutral_ratio': round(neutral_count / total_valid if total_valid > 0 else 0, 4),
            'high_freq_words': high_freq_words,
            'max_praise': max_praise,
            'avg_praise': avg_praise,
            'top_comment': top_comment,
            'top_praise': top_praise
        }

        print(f"  - 评论抓取完成！共获取 {summary['total_comments']} 条有效评论（热评+按比例普通评论），最高点赞：{summary['max_praise']}")
        return comments_df, summary

    except Exception as e:
        print(f"    - 评论获取整体失败: {str(e)}")
        return pd.DataFrame(), {
            'total_comments': 0, 'positive_count': 0, 'negative_count': 0, 'neutral_count': 0,
            'positive_ratio': 0.0, 'negative_ratio': 0.0, 'neutral_ratio': 0.0,
            'high_freq_words': "无", 'max_praise': 0, 'avg_praise': 0.0,
            'top_comment': "无", 'top_praise': 0
        }

# --- 数据保存函数 ---
def save_song_data_for_playlist(song_info, comments_df, summary, playlist_name):
    """保存单首歌的评论数据（汇总表+详细评论表）"""
    # 创建保存目录
    playlist_dir = os.path.join(ROOT_RESULT_DIR, playlist_name)
    detailed_comment_dir = os.path.join(playlist_dir, "detailed_comments")
    os.makedirs(detailed_comment_dir, exist_ok=True)

    # 汇总表路径（每首歌一行，包含统计信息）
    dataset_csv_path = os.path.join(playlist_dir, f"{playlist_name}_dataset.csv")

    # 汇总表数据（避免特殊字符导致CSV格式错乱）
    dataset_row = {
        '歌曲ID': song_info['id'].strip(),
        '歌曲名称': song_info['name'].strip().replace(",", "，"),
        '歌手': song_info['artists'].strip().replace(",", "，"),
        '抓取时间': time.strftime("%Y-%m-%d %H:%M:%S"),
        '评论总数': summary.get('total_comments', 0),
        '积极评论数': summary.get('positive_count', 0),
        '消极评论数': summary.get('negative_count', 0),
        '中立评论数': summary.get('neutral_count', 0),
        '积极评论占比': summary.get('positive_ratio', 0),
        '消极评论占比': summary.get('negative_ratio', 0),
        '中立评论占比': summary.get('neutral_ratio', 0),
        '高频字眼': summary.get('high_freq_words', '无').replace(",", "，"),
        '最高点赞量': summary.get('max_praise', 0),
        '平均点赞量': summary.get('avg_praise', 0),
        'Top1评论预览': summary.get('top_comment', '无').replace(",", "，"),
        'Top1点赞数': summary.get('top_praise', 0)
    }

    # 保存汇总表（不存在则创建，存在则追加）
    if not os.path.exists(dataset_csv_path):
        pd.DataFrame([dataset_row]).to_csv(dataset_csv_path, index=False, encoding='utf-8-sig')
    else:
        pd.DataFrame([dataset_row]).to_csv(dataset_csv_path, mode='a', header=False, index=False, encoding='utf-8-sig')

    # 保存详细评论表（每首歌一个CSV文件，保留所有采集到的评论）
    if not comments_df.empty:
        # 重命名列名（中文更易读），并删除commentId列
        comments_df_renamed = comments_df.drop(columns=['commentId']).rename(columns={
            'user_name': '用户名',
            'user_city': '用户城市',
            'comment': '评论内容',
            'praise': '点赞数',
            'date': '评论时间',
            'sentiment': '情感得分'
        }).copy()
        # 确保点赞数为整数类型
        comments_df_renamed['点赞数'] = comments_df_renamed['点赞数'].astype(int)
        # 保存文件
        comment_csv_path = os.path.join(detailed_comment_dir, f"comments_{song_info['id']}.csv")
        comments_df_renamed.to_csv(comment_csv_path, index=False, encoding='utf-8-sig', escapechar='\\')

# --- 清空历史数据函数 ---
def clear_playlist_history_data(playlist_name):
    """清空指定歌单的历史抓取数据（避免重复）"""
    playlist_dir = os.path.join(ROOT_RESULT_DIR, playlist_name)
    if os.path.exists(playlist_dir):
        # 删除详细评论目录
        detailed_comment_dir = os.path.join(playlist_dir, "detailed_comments")
        if os.path.exists(detailed_comment_dir):
            shutil.rmtree(detailed_comment_dir)
            print(f"  - 已删除历史详细评论目录: {detailed_comment_dir}")
        # 删除汇总表文件
        dataset_csv_path = os.path.join(playlist_dir, f"{playlist_name}_dataset.csv")
        if os.path.exists(dataset_csv_path):
            os.remove(dataset_csv_path)
            print(f"  - 已删除历史汇总表文件: {dataset_csv_path}")
    else:
        print(f"  - 歌单目录 '{playlist_dir}' 不存在，无需清空")

# --- 主函数 ---
def main():
    """主执行函数：遍历歌单，按比例抓取每首歌的评论并保存"""
    # 创建根结果目录
    os.makedirs(ROOT_RESULT_DIR, exist_ok=True)
    print(f"\n{'=' * 40} 开始执行歌单评论抓取任务（优化版） {'=' * 40}")

    # 遍历每个歌单
    for playlist in PLAYLISTS:
        playlist_id, playlist_name = playlist["id"], playlist["name"]

        # 清空历史数据
        print(f"\n[清空历史数据] 正在处理歌单: {playlist_name}")
        clear_playlist_history_data(playlist_name)

        # 开始处理当前歌单
        print(f"\n{'=' * 20} 开始处理歌单: {playlist_name} (ID: {playlist_id}) {'=' * 20}")
        # 获取歌单中的所有歌曲
        song_list = get_playlist_tracks(playlist_id, playlist_name)
        if not song_list:
            print(f"跳过歌单: {playlist_name}，未获取到有效歌曲。")
            continue

        # 遍历每首歌，按比例抓取评论
        print(f"准备处理 {len(song_list)} 首歌曲的评论...")
        for song_info in tqdm(song_list, desc=f"处理 {playlist_name} 的歌曲"):
            print(f"\n{'=' * 10} 正在处理歌曲: {song_info['name']} - {song_info['artists']} (ID: {song_info['id']}) {'=' * 10}")
            # 抓取评论（纯比例，不截断），传递歌单名称
            comments_df, summary = fetch_comments_detailed(song_info['id'], playlist_name)
            # 保存数据（保留所有采集到的评论）
            save_song_data_for_playlist(song_info, comments_df, summary, playlist_name)
            if not comments_df.empty and summary:
                print(f"  - 成功保存 {summary['total_comments']} 条评论（按总评论数比例采集），Top1点赞：{summary['top_praise']}")
            else:
                print(f"  - 未获取到有效评论，仅在汇总表记录歌曲信息。")
            # 控制歌曲间的请求间隔
            time.sleep(random.uniform(REQUEST_DELAY * 0.8, REQUEST_DELAY * 1.5))

        print(f"\n{'=' * 20} 歌单 '{playlist_name}' 处理完毕 {'=' * 20}")

    print(f"\n{'=' * 40} 所有歌单处理完成！结果保存在 '{ROOT_RESULT_DIR}' 目录下 {'=' * 40}")

# --- 定时任务函数 ---
def start_daily_schedule():
    """启动每日定时任务（默认每天02:00执行）"""
    schedule.every().day.at(DAILY_UPDATE_TIME).do(main)
    print(f"\n定时任务已启动！每日 {DAILY_UPDATE_TIME} 自动执行评论抓取（优化版）")
    print("提示：如需停止程序，按 Ctrl+C 中断")
    # 持续运行定时任务
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每60秒检查一次是否有任务需要执行

# --- 程序入口 ---
if __name__ == "__main__":
    # 方式1：立即执行一次抓取任务（适合测试）
    main()

    # 方式2：立即执行一次 + 启动每日定时任务（适合长期运行）
    # main()
    # start_daily_schedule()

    # 方式3：仅启动每日定时任务（适合服务器部署）
    # start_daily_schedule()