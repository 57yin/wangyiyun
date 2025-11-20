import subprocess
import sys
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import streamlit as st
from collections import Counter
import jieba

# 依赖安装相关函数（保持不变）
def install_deps():
    required_packages = ['streamlit>=1.28.0', 'pandas', 'plotly', 'openpyxl', 'numpy', 'jieba']
    try:
        import pkg_resources
        installed = {p.key for p in pkg_resources.working_set}
        print(f"正在检查并安装/升级依赖库: {', '.join(required_packages)}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", *required_packages])
        print("依赖库安装/升级完成。")
    except Exception as e:
        print(f"自动安装依赖失败: {e}")
        print("请手动安装以下库: " + ", ".join(required_packages))

# 首次运行依赖检查（保持不变）
try:
    from importlib.metadata import version
    st_version = version('streamlit')
    print(f"当前 Streamlit 版本: {st_version}")
    if tuple(map(int, st_version.split('.'))) < (1, 28, 0):
        print("Streamlit 版本过低，需要升级...")
        raise ImportError("Streamlit version too old")
except (ImportError, Exception):
    print("检测到缺失依赖或版本不兼容，正在尝试自动安装...")
    install_deps()
    # 重新导入所有库
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import numpy as np
    import streamlit as st
    from collections import Counter
    import jieba

# ---------------------- 全局配置 ----------------------
st.set_page_config(
    page_title="网易云歌单+榜单评论综合数据分析工具",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"  
)

# 自定义样式（保持不变，注意后续若仍有布局冲突可简化调试）
custom_style = """
    <style>
        /* 全局重置与基础样式 */
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        
        /* 页面背景渐变 */
        .main {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        /* 隐藏默认菜单和页脚 */
        #MainMenu {visibility: hidden !important;}
        footer {visibility: hidden !important;}
        header {visibility: hidden !important;}
        
        /* 其他样式保持不变... */
    </style>
"""
st.markdown(custom_style, unsafe_allow_html=True)

# 颜色配置（保持不变）
COLOR_PALETTE = {
    'primary': '#1DB954',      
    'primary_light': '#1ed760',
    'primary_dark': '#1a9e48', 
    'secondary': '#FF6B6B',    
    'accent': '#4ECDC4',       
    'background': '#F8F9FA',   
    'text': '#2d3436',         
    'light_text': '#6c757d',   
    'card_bg': '#FFFFFF',      
    'success': '#28a745',      
    'warning': '#ffc107',      
    'danger': '#dc3545',       
    'info': '#17a2b8'          
}

# 情感分析阈值（保持不变）
NEGATIVE_THRESHOLD = 0.4  
POSITIVE_THRESHOLD = 0.6  

# 数据源配置（保持不变）
TYPE_LIST_STYLE = ['流行', '热血', '00后', '华语', '伤感', '夜晚', '治愈', '放松', '感动', '安静', '民谣', '孤独', '浪漫']
TYPE_LIST_RANK = ['热歌榜', '新歌榜', '飙升榜', '原创榜']
DATA_DIR = Path(__file__).parent  
RANK_DATA_ROOT = "multi_playlist_results"  

# ---------------------- 数据加载与预处理模块（保持不变） ----------------------
def load_style_playlist_data():
    all_data = []
    found_files = []
    skipped_files = []
    for cat in TYPE_LIST_STYLE:
        file_path = DATA_DIR / f"{cat}.csv"
        if file_path.exists():
            try:
                df = pd.read_csv(file_path, index_col=0, on_bad_lines='skip')
                if df.empty:
                    skipped_files.append(f"{cat}.csv (文件为空)")
                    continue
                required_columns = ['名称', '创建日期', '播放次数', '收藏量', '转发量', '评论数', '歌单长度', 'tag1']
                if not all(col in df.columns for col in required_columns):
                    missing_cols = [col for col in required_columns if col not in df.columns]
                    skipped_files.append(f"{cat}.csv (缺少列: {', '.join(missing_cols)})")
                    continue
                df['分类'] = cat.strip()
                all_data.append(df)
                found_files.append(cat)
            except Exception as e:
                skipped_files.append(f"{cat}.csv (读取错误: {str(e)})")
        else:
            skipped_files.append(f"{cat}.csv (文件不存在)")
    if not all_data:
        return pd.DataFrame(), found_files, skipped_files, 0
    combined_df = pd.concat(all_data, ignore_index=True)
    duplicate_cols = ['名称', '分类', '创建日期']
    before_count = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=duplicate_cols, keep='first')
    after_count = len(combined_df)
    dup_count = before_count - after_count
    combined_df['创建日期'] = pd.to_datetime(combined_df['创建日期'], errors='coerce')
    numeric_cols = ['播放次数', '收藏量', '转发量', '评论数', '歌单长度']
    for col in numeric_cols:
        combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0).astype(int)
    combined_df['tag1'] = combined_df['tag1'].str.replace('nan', '').str.strip()
    combined_df['收藏播放比'] = (combined_df['收藏量'] / combined_df['播放次数'] * 100).round(4)
    combined_df['评论播放比'] = (combined_df['评论数'] / combined_df['播放次数'] * 100).round(4)
    combined_df['创建月份'] = combined_df['创建日期'].dt.to_period('M')
    return combined_df, found_files, skipped_files, dup_count

def load_rank_comment_data():
    all_rank_data = []
    found_ranks = []
    skipped_ranks = []
    for rank_name in TYPE_LIST_RANK:
        rank_dir = DATA_DIR / RANK_DATA_ROOT / rank_name
        dataset_path = rank_dir / f"{rank_name}_dataset.csv"
        comment_dir = rank_dir / "detailed_comments"  
        if dataset_path.exists():
            try:
                df = pd.read_csv(dataset_path, on_bad_lines='skip', encoding='utf-8-sig')
                if df.empty:
                    skipped_ranks.append(f"{rank_name} (文件为空)")
                    continue
                required_columns = ['歌曲ID', '歌曲名称', '歌手', '评论总数', '积极评论数', '消极评论数', '中立评论数', '积极评论占比', '消极评论占比', '中立评论占比', '高频字眼']
                missing_cols = [col for col in required_columns if col not in df.columns]
                if missing_cols:
                    skipped_ranks.append(f"{rank_name} (缺少列: {', '.join(missing_cols)})")
                    continue
                df['评论文件路径'] = df['歌曲ID'].apply(
                    lambda song_id: str(comment_dir / f"comments_{song_id}.csv") if (comment_dir / f"comments_{song_id}.csv").exists() else ""
                )
                df['榜单类型'] = rank_name.strip()
                all_rank_data.append(df)
                found_ranks.append(rank_name)
            except Exception as e:
                skipped_ranks.append(f"{rank_name} (读取错误: {str(e)})")
        else:
            skipped_ranks.append(f"{rank_name} (数据集文件不存在)")
    if not all_rank_data:
        return pd.DataFrame(), found_ranks, skipped_ranks
    combined_df = pd.concat(all_rank_data, ignore_index=True)
    numeric_cols = ['评论总数', '积极评论数', '消极评论数', '中立评论数', '积极评论占比', '消极评论占比', '中立评论占比']
    for col in numeric_cols:
        combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0)
    combined_df['情感倾向'] = combined_df.apply(
        lambda x: '积极' if x['积极评论占比'] > x['消极评论占比'] and x['积极评论占比'] > 0.3 
                  else '消极' if x['消极评论占比'] > x['积极评论占比'] and x['消极评论占比'] > 0.3
                  else '中立', axis=1
    )
    return combined_df, found_ranks, skipped_ranks

def load_all_data(selected_data_source):
    if selected_data_source == "13类风格歌单数据":
        df, found, skipped, dup_count = load_style_playlist_data()
        load_summary = {
            "data_type": "风格歌单",
            "found_count": len(found),
            "total_count": len(TYPE_LIST_STYLE),
            "found_items": found,
            "skipped_items": skipped,
            "dup_count": dup_count
        }
    else:
        df, found, skipped = load_rank_comment_data()
        load_summary = {
            "data_type": "榜单评论",
            "found_count": len(found),
            "total_count": len(TYPE_LIST_RANK),
            "found_items": found,
            "skipped_items": skipped,
            "dup_count": 0
        }
    return df, load_summary

# ---------------------- 数据概览卡片（保持不变） ----------------------
def display_data_overview(df, data_source):
    st.markdown('<div class="sub-title">📈 数据概览</div>', unsafe_allow_html=True)
    if data_source == "13类风格歌单数据":
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #1DB954;">总歌单数量</h4>
                <p style="font-size: 24px; font-weight: bold;">{:,}</p>
            </div>
            """.format(len(df)), unsafe_allow_html=True)
        with col2:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #FF6B6B;">总播放次数</h4>
                <p style="font-size: 24px; font-weight: bold;">{:,}</p>
            </div>
            """.format(df['播放次数'].sum()), unsafe_allow_html=True)
        with col3:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #4ECDC4;">总收藏量</h4>
                <p style="font-size: 24px; font-weight: bold;">{:,}</p>
            </div>
            """.format(df['收藏量'].sum()), unsafe_allow_html=True)
        with col4:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #9B59B6;">平均歌单长度</h4>
                <p style="font-size: 24px; font-weight: bold;">{:.1f}</p>
            </div>
            """.format(df['歌单长度'].mean()), unsafe_allow_html=True)
        col5, col6, col7 = st.columns(3)
        with col5:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #F39C12;">总评论数</h4>
                <p style="font-size: 24px; font-weight: bold;">{:,}</p>
            </div>
            """.format(df['评论数'].sum()), unsafe_allow_html=True)
        with col6:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #8E44AD;">总转发量</h4>
                <p style="font-size: 24px; font-weight: bold;">{:,}</p>
            </div>
            """.format(df['转发量'].sum()), unsafe_allow_html=True)
        with col7:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #16A085;">平均收藏播放比(%)</h4>
                <p style="font-size: 24px; font-weight: bold;">{:.2f}</p>
            </div>
            """.format(df['收藏播放比'].mean()), unsafe_allow_html=True)
    else:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #1DB954;">总歌曲数量</h4>
                <p style="font-size: 24px; font-weight: bold;">{:,}</p>
            </div>
            """.format(len(df)), unsafe_allow_html=True)
        with col2:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #FF6B6B;">总评论数</h4>
                <p style="font-size: 24px; font-weight: bold;">{:,}</p>
            </div>
            """.format(df['评论总数'].sum()), unsafe_allow_html=True)
        with col3:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #4ECDC4;">平均积极评论占比(%)</h4>
                <p style="font-size: 24px; font-weight: bold;">{:.2f}</p>
            </div>
            """.format(df['积极评论占比'].mean() * 100), unsafe_allow_html=True)
        with col4:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #9B59B6;">积极情感歌曲数</h4>
                <p style="font-size: 24px; font-weight: bold;">{:,}</p>
            </div>
            """.format(len(df[df['情感倾向'] == '积极'])), unsafe_allow_html=True)
        col5, col6, col7 = st.columns(3)
        with col5:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #F39C12;">消极情感歌曲数</h4>
                <p style="font-size: 24px; font-weight: bold;">{:,}</p>
            </div>
            """.format(len(df[df['情感倾向'] == '消极'])), unsafe_allow_html=True)
        with col6:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #8E44AD;">中立情感歌曲数</h4>
                <p style="font-size: 24px; font-weight: bold;">{:,}</p>
            </div>
            """.format(len(df[df['情感倾向'] == '中立'])), unsafe_allow_html=True)
        with col7:
            st.markdown("""
            <div class="metric-card">
                <h4 style="color: #16A085;">平均单首歌曲评论数</h4>
                <p style="font-size: 24px; font-weight: bold;">{:.1f}</p>
            </div>
            """.format(df['评论总数'].mean()), unsafe_allow_html=True)

# ---------------------- 高级可视化模块（完整代码） ----------------------
def plot_style_playlist_visualizations(df):
    """13类风格歌单可视化"""
    if df.empty:
        st.warning("没有可供可视化的风格歌单数据")
        return
    
    st.markdown('<div class="sub-title">🎯 风格歌单深度分析</div>', unsafe_allow_html=True)
    
    # 创建标签页
    tab1, tab2, tab3, tab4 = st.tabs(['分类分析', '时间趋势', '相关性分析', '高级洞察'])
    
    # Tab 1: 分类分析
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # 各分类歌单数量
            cat_counts = df['分类'].value_counts()
            fig = px.bar(
                x=cat_counts.index,
                y=cat_counts.values,
                title='各分类歌单数量分布',
                labels={'x': '分类', 'y': '歌单数量'},
                color=cat_counts.values,
                color_continuous_scale='Reds',
                template='plotly_white'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # 各分类平均播放量
            avg_play = df.groupby('分类')['播放次数'].mean().sort_values(ascending=False)
            fig = px.bar(
                x=avg_play.index,
                y=avg_play.values,
                title='各分类平均播放量',
                labels={'x': '分类', 'y': '平均播放次数'},
                color=avg_play.values,
                color_continuous_scale='Blues',
                template='plotly_white'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # 各分类综合指标雷达图
        st.markdown("### 各分类综合表现对比")
        top_categories = df['分类'].value_counts().head(6).index
        cat_metrics = df[df['分类'].isin(top_categories)].groupby('分类').agg({
            '播放次数': 'mean',
            '收藏量': 'mean',
            '评论数': 'mean',
            '歌单长度': 'mean'
        }).reset_index()
        
        # 数据标准化
        for col in ['播放次数', '收藏量', '评论数', '歌单长度']:
            cat_metrics[col] = (cat_metrics[col] - cat_metrics[col].min()) / (cat_metrics[col].max() - cat_metrics[col].min())
        
        fig = go.Figure()
        for _, row in cat_metrics.iterrows():
            fig.add_trace(go.Scatterpolar(
                r=[row['播放次数'], row['收藏量'], row['评论数'], row['歌单长度']],
                theta=['播放次数', '收藏量', '评论数', '歌单长度'],
                name=row['分类']
            ))
        
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            showlegend=True,
            height=500,
            template='plotly_white'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Tab 2: 时间趋势
    with tab2:
        # 按月份统计歌单创建数量
        monthly_trend = df.groupby('创建月份').size().reset_index(name='歌单数量')
        monthly_trend['创建月份'] = monthly_trend['创建月份'].astype(str)
        
        fig = px.line(
            monthly_trend,
            x='创建月份',
            y='歌单数量',
            title='歌单创建时间趋势',
            labels={'创建月份': '月份', '歌单数量': '新增歌单数量'},
            template='plotly_white',
            markers=True
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # 近6个月各分类歌单增长情况
        recent_months = df['创建月份'].unique()[-6:] if len(df['创建月份'].unique()) >=6 else df['创建月份'].unique()
        recent_data = df[df['创建月份'].isin(recent_months)]
        
        if len(recent_data) > 0:
            monthly_cat = recent_data.groupby(['创建月份', '分类']).size().reset_index(name='歌单数量')
            monthly_cat['创建月份'] = monthly_cat['创建月份'].astype(str)
            
            fig = px.area(
                monthly_cat,
                x='创建月份',
                y='歌单数量',
                color='分类',
                title='近6个月各分类歌单增长趋势',
                labels={'创建月份': '月份', '歌单数量': '歌单数量'},
                template='plotly_white'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Tab 3: 相关性分析
    with tab3:
        col1, col2 = st.columns(2)
        
        with col1:
            # 播放量vs收藏量散点图
            fig = px.scatter(
                df,
                x='播放次数',
                y='收藏量',
                color='分类',
                size='歌单长度',
                hover_data=['名称', '创建日期'],
                title='播放量 vs 收藏量',
                labels={'播放次数': '播放次数', '收藏量': '收藏量'},
                opacity=0.7,
                template='plotly_white'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # 播放量vs评论数散点图
            fig = px.scatter(
                df,
                x='播放次数',
                y='评论数',
                color='分类',
                size='收藏量',
                hover_data=['名称', '创建日期'],
                title='播放量 vs 评论数',
                labels={'播放次数': '播放次数', '评论数': '评论数'},
                opacity=0.7,
                template='plotly_white'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # 数值特征相关性热力图
        numeric_features = ['播放次数', '收藏量', '转发量', '评论数', '歌单长度', '收藏播放比', '评论播放比']
        corr_matrix = df[numeric_features].corr()
        
        fig = px.imshow(
            corr_matrix,
            title='特征相关性热力图',
            labels=dict(color='相关系数'),
            x=numeric_features,
            y=numeric_features,
            color_continuous_scale='RdBu_r',
            template='plotly_white'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    # Tab 4: 高级洞察
    with tab4:
        # Top 10 高收藏播放比歌单
        st.markdown("### Top 10 高收藏率歌单")
        # 过滤掉播放次数为0的歌单，避免除以零错误
        high_fav_ratio_df = (
            df[df['播放次数'] > 1000]
            .sort_values('收藏播放比', ascending=False)
            .drop_duplicates(subset='名称', keep='first')
            .nlargest(10, '收藏播放比')
            [['名称', '分类', '播放次数', '收藏量', '收藏播放比', '创建日期']]
        )
        
        fig = px.bar(
            high_fav_ratio_df,
            x='名称',
            y='收藏播放比',
            color='分类',
            title='收藏率最高的10个歌单 (收藏量/播放量%)',
            labels={'名称': '歌单名称', '收藏播放比': '收藏率(%)'},
            template='plotly_white',
            hover_data=['播放次数', '收藏量', '创建日期']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # 歌单长度分布
        st.markdown("### 歌单长度分布")
        fig = px.histogram(
            df,
            x='歌单长度',
            nbins=30,
            title='歌单长度分布',
            labels={'歌单长度': '歌曲数量', 'count': '歌单数量'},
            color_discrete_sequence=['#4ECDC4'],
            template='plotly_white'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # 标签云（使用Plotly的条形图模拟）
        st.markdown("### 热门标签分析")
        if 'tag1' in df.columns:
            # 过滤掉空标签
            tag_counts = df['tag1'].replace('', pd.NA).dropna().value_counts().head(15)
            if not tag_counts.empty:
                fig = px.bar(
                    x=tag_counts.values,
                    y=tag_counts.index,
                    orientation='h',
                    title='热门标签 Top 15',
                    labels={'x': '出现次数', 'y': '标签'},
                    color=tag_counts.values,
                    color_continuous_scale='Oranges',
                    template='plotly_white'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("没有找到有效的标签数据。")
        else:
            st.warning("数据中缺少 'tag1' 列，无法进行热门标签分析。")

from wordcloud import WordCloud
import matplotlib.pyplot as plt

def plot_rank_comment_visualizations(df):
    """4类榜单歌曲评论可视化"""
    if df.empty:
        st.warning("没有可供可视化的榜单评论数据")
        return
    
    st.markdown('<div class="sub-title">🎯 榜单歌曲评论深度分析</div>', unsafe_allow_html=True)
    
    # 创建标签页
    tab1, tab2, tab3, tab4 = st.tabs(['情感分析', '评论量分析', '高频词分析', '高级洞察'])
   
    # Tab 1: 情感分析
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # 各榜单情感倾向分布
            sentiment_counts = df.groupby(['榜单类型', '情感倾向']).size().reset_index(name='歌曲数量')
            fig = px.bar(
                sentiment_counts,
                x='榜单类型',
                y='歌曲数量',
                color='情感倾向',
                barmode='group',
                title='各榜单歌曲情感倾向分布',
                labels={'歌曲数量': '歌曲数量', '榜单类型': '榜单类型'},
                color_discrete_map={'积极': '#2ECC40', '消极': '#FF4136', '中立': '#AAAAAA'},
                template='plotly_white'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # 各榜单平均情感占比
            avg_sentiment = df.groupby('榜单类型').agg({
                '积极评论占比': 'mean',
                '消极评论占比': 'mean',
                '中立评论占比': 'mean'
            }).reset_index()
            
            fig = px.line(
                avg_sentiment,
                x='榜单类型',
                y=['积极评论占比', '消极评论占比', '中立评论占比'],
                title='各榜单平均情感占比趋势',
                labels={'value': '平均占比', 'variable': '情感类型'},
                template='plotly_white',
                markers=True
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # 情感得分分布箱线图
        st.markdown("### 各榜单情感得分分布")
        fig = px.box(
            df,
            x='榜单类型',
            y=['积极评论占比', '消极评论占比'],
            title='各榜单情感得分分布箱线图',
            labels={'value': '情感占比', 'variable': '情感类型'},
            color_discrete_map={'积极评论占比': '#2ECC40', '消极评论占比': '#FF4136'},
            template='plotly_white'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    # Tab 2: 评论量分析
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            # 各榜单评论总数分布
            fig = px.histogram(
                df,
                x='评论总数',
                color='榜单类型',
                title='各榜单歌曲评论总数分布',
                labels={'评论总数': '评论总数', 'count': '歌曲数量'},
                template='plotly_white',
                opacity=0.7
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # 各榜单平均评论数
            avg_comments = df.groupby('榜单类型')['评论总数'].agg(['mean', 'median', 'max']).reset_index()
            fig = px.bar(
                avg_comments,
                x='榜单类型',
                y=['mean', 'median', 'max'],
                title='各榜单歌曲评论数统计',
                labels={'value': '评论数', 'variable': '统计类型'},
                barmode='group',
                template='plotly_white'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # 评论数与情感倾向关系
        st.markdown("### 评论数与情感倾向关系")
        fig = px.scatter(
            df,
            x='评论总数',
            y='积极评论占比',
            color='榜单类型',
            size='消极评论占比',
            hover_data=['歌曲名称', '歌手'],
            title='评论总数 vs 积极评论占比',
            labels={'评论总数': '评论总数', '积极评论占比': '积极评论占比'},
            template='plotly_white',
            opacity=0.7
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Tab 3: 高频词分析
    with tab3:
        # 合并所有高频词
        all_keywords = []
        for keywords in df['高频字眼'].dropna():
            if keywords and keywords != '':
                all_keywords.extend([kw.strip() for kw in keywords.split(',') if kw.strip()])
        
        if all_keywords:
            # 新增：将高频词列表转换为文本字符串
            keywords_text = ' '.join(all_keywords)  # 用空格连接高频词，供词云使用
            
            keyword_counts = Counter(all_keywords).most_common(20)
            keywords_df = pd.DataFrame(keyword_counts, columns=['关键词', '出现次数'])
            
            # 生成词云
            font_path = 'C:/Windows/Fonts/simsun.ttc'

            wordcloud = WordCloud(
                font_path=font_path,  # 新增字体路径参数，让 WordCloud 用指定字体渲染
                width=800,
                height=400,
                background_color='white',
                colormap='viridis',
                max_words=100,
                max_font_size=100,
                contour_width=3,
                contour_color=COLOR_PALETTE['primary']
            ).generate(keywords_text)  # 现在 keywords_text 已定义
            
            # 显示词云
            st.markdown("### 高频词云图")
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(wordcloud, interpolation='bilinear')
            ax.axis('off')
            st.pyplot(fig)
            
            # 原来的高频词条形图和榜单对比
            keyword_counts = Counter(all_keywords).most_common(20)
            keywords_df = pd.DataFrame(keyword_counts, columns=['关键词', '出现次数'])
            
            col1, col2 = st.columns(2)
            
            with col1:
                # 高频词词云（条形图模拟）
                fig = px.bar(
                    keywords_df,
                    x='出现次数',
                    y='关键词',
                    orientation='h',
                    title='所有歌曲高频关键词 Top 20',
                    labels={'出现次数': '出现次数', '关键词': '关键词'},
                    color='出现次数',
                    color_continuous_scale='Viridis',
                    template='plotly_white'
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # 各榜单高频词对比（取前5个）
                st.markdown("### 各榜单Top5高频词")
                rank_keywords = {}
                
                for rank in df['榜单类型'].unique():
                    rank_df = df[df['榜单类型'] == rank]
                    rank_keywords_list = []
                    
                    for keywords in rank_df['高频字眼'].dropna():
                        if keywords and keywords != '':
                            rank_keywords_list.extend([kw.strip() for kw in keywords.split(',') if kw.strip()])
                    
                    if rank_keywords_list:
                        rank_keywords[rank] = Counter(rank_keywords_list).most_common(5)
                
                # 创建表格显示
                for rank, keywords in rank_keywords.items():
                    st.subheader(f"{rank}")
                    kw_df = pd.DataFrame(keywords, columns=['关键词', '出现次数'])
                    st.dataframe(kw_df, use_container_width=True)
        else:
            st.info("没有找到有效的高频词数据")
  
    # Tab 4: 高级洞察
    with tab4:
        # Top 10 积极评论占比最高的歌曲
        st.markdown("### Top 10 积极评论占比最高的歌曲")
        top_positive = df.nlargest(10, '积极评论占比')[['歌曲名称', '歌手', '榜单类型', '积极评论占比', '评论总数', '高频字眼']]
        
        fig = px.bar(
            top_positive,
            x='歌曲名称',
                            y='积极评论占比',
                color='榜单类型',
                title='积极评论占比最高的10首歌曲',
                labels={'歌曲名称': '歌曲名称', '积极评论占比': '积极评论占比'},
                template='plotly_white',
                hover_data=['歌手', '评论总数', '高频字眼']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
            
            # Top 10 消极评论占比最高的歌曲
        st.markdown("### Top 10 消极评论占比最高的歌曲")
        top_negative = df.nlargest(10, '消极评论占比')[['歌曲名称', '歌手', '榜单类型', '消极评论占比', '评论总数', '高频字眼']]
            
        fig = px.bar(
            top_negative,
            x='歌曲名称',       
            y='消极评论占比',
            color='榜单类型',
            title='消极评论占比最高的10首歌曲',
            labels={'歌曲名称': '歌曲名称', '消极评论占比': '消极评论占比'},
            template='plotly_white',
            hover_data=['歌手', '评论总数', '高频字眼'],
            color_discrete_map={'热歌榜': '#FF4136', '新歌榜': '#FF851B', '飙升榜': '#FFDC00', '原创榜': '#B10DC9'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
            
            # 各榜单歌曲情感特征雷达图
        st.markdown("### 各榜单情感特征对比")
        rank_sentiment = df.groupby('榜单类型').agg({
            '积极评论占比': 'mean',
            '消极评论占比': 'mean',
            '中立评论占比': 'mean',
            '评论总数': 'mean'
        }).reset_index()
            
            # 数据标准化
        for col in ['积极评论占比', '消极评论占比', '中立评论占比', '评论总数']:
            rank_sentiment[col] = (rank_sentiment[col] - rank_sentiment[col].min()) / (rank_sentiment[col].max() - rank_sentiment[col].min())
            
            fig = go.Figure()
            for _, row in rank_sentiment.iterrows():
                fig.add_trace(go.Scatterpolar(
                    r=[row['积极评论占比'], row['消极评论占比'], row['中立评论占比'], row['评论总数']],
                    theta=['积极评论占比', '消极评论占比', '中立评论占比', '平均评论数'],
                    name=row['榜单类型']
                ))
            
            fig.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
                showlegend=True,
                height=500,
                template='plotly_white'
            )
            st.plotly_chart(fig, use_container_width=True)

# ---------------------- 主界面布局与逻辑 ----------------------
def main():
    # 页面标题
    st.markdown('<div class="page-title">🎵 网易云歌单+榜单评论综合数据分析工具</div>', unsafe_allow_html=True)
    st.markdown("---")
    
    # 数据源选择
    selected_data_source = st.selectbox(
        "请选择要分析的数据源",
        ["13类风格歌单数据", "4类榜单歌曲评论数据"]
    )
    
    # 加载数据（使用st.spinner显示加载状态）
    with st.spinner("正在加载数据，请稍候..."):
        df, load_summary = load_all_data(selected_data_source)
    
    # 显示加载状态
    if not df.empty:
        st.success(f"✅ 成功加载 {load_summary['found_count']} / {load_summary['total_count']} 个{load_summary['data_type']}数据")
        if load_summary['dup_count'] > 0:
            st.info(f"🔍 数据去重完成：共移除 {load_summary['dup_count']} 条重复数据")
    else:
        st.warning("⚠️ 数据加载失败或没有找到有效数据")
    
    # 显示跳过的文件
    if load_summary['skipped_items']:
        with st.expander("⚠️ 查看被跳过的文件", expanded=False):
            for item in load_summary['skipped_items']:
                st.write(item)
    
    st.markdown("---")
    
    # 显示数据概览
    if not df.empty:
        display_data_overview(df, selected_data_source)
    st.markdown("---")
    
    # --- 核心修改：将筛选条件从侧边栏移至主页面 ---
    filtered_df = pd.DataFrame()
    if not df.empty:
        st.markdown('<div class="sub-title">🔍 筛选条件</div>', unsafe_allow_html=True)
        
        # 使用expander组件来容纳所有筛选器，保持页面整洁
        with st.expander("展开/折叠筛选器", expanded=True):
            if selected_data_source == "13类风格歌单数据":
                # 创建一个2列的布局来放置筛选器
                col1, col2 = st.columns(2)
                
                with col1:
                    # 歌单分类筛选
                    selected_cats = st.multiselect(
                        "歌单分类", 
                        options=df['分类'].unique(), 
                        default=df['分类'].unique()
                    )
                    
                    # 播放次数筛选
                    play_min, play_max = st.slider(
                        "播放次数范围",
                        min_value=int(df['播放次数'].min()),
                        max_value=int(df['播放次数'].max()),
                        value=(int(df['播放次数'].min()), int(df['播放次数'].max()))
                    )
                    
                    # 收藏量筛选
                    fav_min = st.number_input(
                        "最小收藏量", 
                        min_value=0, 
                        max_value=int(df['收藏量'].max()), 
                        value=0
                    )
                
                with col2:
                    # 日期筛选
                    has_dates = not df['创建日期'].isna().all()
                    date_min_ts, date_max_ts = None, None
                    if has_dates:
                        date_min, date_max = st.date_input(
                            "创建日期范围",
                            value=(df['创建日期'].min(), df['创建日期'].max()),
                            min_value=df['创建日期'].min(),
                            max_value=df['创建日期'].max()
                        )
                        date_min_ts = pd.to_datetime(date_min)
                        date_max_ts = pd.to_datetime(date_max)
                    
                    # 歌单长度筛选
                    len_min, len_max = st.slider(
                        "歌单歌曲数量",
                        min_value=1,
                        max_value=int(df['歌单长度'].max()),
                        value=(1, int(df['歌单长度'].max()))
                    )
                
                # 应用筛选
                filtered_df = df[
                    (df['分类'].isin(selected_cats)) &
                    (df['播放次数'] >= play_min) &
                    (df['播放次数'] <= play_max) &
                    (df['收藏量'] >= fav_min) &
                    (df['歌单长度'] >= len_min) &
                    (df['歌单长度'] <= len_max)
                ].copy()
                
                if has_dates and date_min_ts and date_max_ts:
                    filtered_df = filtered_df[
                        (filtered_df['创建日期'] >= date_min_ts) &
                        (filtered_df['创建日期'] <= date_max_ts)
                    ]
            
            else: # 4类榜单歌曲评论数据
                # 创建一个2列的布局来放置筛选器
                col1, col2 = st.columns(2)
                
                with col1:
                    # 榜单类型筛选
                    selected_ranks = st.multiselect(
                        "榜单类型",
                        options=df['榜单类型'].unique(),
                        default=df['榜单类型'].unique()
                    )
                    
                    # 评论数筛选
                    comment_min, comment_max = st.slider(
                        "评论总数范围",
                        min_value=int(df['评论总数'].min()),
                        max_value=int(df['评论总数'].max()),
                        value=(int(df['评论总数'].min()), int(df['评论总数'].max()))
                    )
                
                with col2:
                    # 情感倾向筛选
                    selected_sentiments = st.multiselect(
                        "情感倾向",
                        options=['积极', '消极', '中立'],
                        default=['积极', '消极', '中立']
                    )
                    
                    # 积极评论占比筛选
                    pos_ratio_min, pos_ratio_max = st.slider(
                        "积极评论占比范围",
                        min_value=0.0,
                        max_value=1.0,
                        value=(0.0, 1.0),
                        step=0.01
                    )
                
                # 应用筛选
                filtered_df = df[
                    (df['榜单类型'].isin(selected_ranks)) &
                    (df['评论总数'] >= comment_min) &
                    (df['评论总数'] <= comment_max) &
                    (df['情感倾向'].isin(selected_sentiments)) &
                    (df['积极评论占比'] >= pos_ratio_min) &
                    (df['积极评论占比'] <= pos_ratio_max)
                ].copy()

    # 显示筛选结果
    if not df.empty:
        st.markdown('<div class="sub-title">📋 筛选结果</div>', unsafe_allow_html=True)
        st.markdown(f"**符合条件的记录数量：{len(filtered_df)}**")
        
        # 显示数据表格
        with st.expander("查看详细数据", expanded=False):
            if selected_data_source == "13类风格歌单数据":
                display_cols = ['名称', '分类', '创建日期', '播放次数', '收藏量', '评论数', '歌单长度', 'tag1']
            else:
                display_cols = ['歌曲名称', '歌手', '榜单类型', '评论总数', '积极评论数', '消极评论数', '中立评论数', '情感倾向', '高频字眼']
            
            # 确保所有要显示的列都存在于filtered_df中
            display_cols = [col for col in display_cols if col in filtered_df.columns]
            st.dataframe(
                filtered_df[display_cols],
                height=400,
                use_container_width=True
            )
        
        # 榜单评论数据专属 - 查看单首歌曲详细评论
        if selected_data_source == "4类榜单歌曲评论数据" and not filtered_df.empty:
            st.markdown("---")
            st.markdown('<div class="sub-title">💬 查看单首歌曲详细评论</div>', unsafe_allow_html=True)
            
            # 下拉选择要查看的歌曲
            song_options = filtered_df.apply(
                lambda x: f"{x['歌曲名称']} - {x['歌手']}（{x['榜单类型']}）", axis=1
            ).tolist()
            if song_options: # 确保列表不为空
                selected_song_idx = st.selectbox("选择歌曲", range(len(song_options)), format_func=lambda i: song_options[i])
                
                # 获取选中歌曲的评论文件路径
                selected_song = filtered_df.iloc[selected_song_idx]
                comment_file_path = selected_song.get('评论文件路径', "") # 使用.get()避免KeyError
                
                if comment_file_path and comment_file_path != "":
                    # 加载评论数据
                    try:
                        comments_df = pd.read_csv(comment_file_path, encoding='utf-8-sig')
                        
                        # 评论筛选功能
                        st.markdown("#### 评论筛选")
                        col1, col2 = st.columns(2)
                        with col1:
                            comment_search = st.text_input("搜索评论内容")
                        with col2:
                            sentiment_filter = st.selectbox("筛选情感倾向", ["全部", "积极", "消极", "中立"])
                        
                        # 应用筛选
                        filtered_comments = comments_df.copy()
                        if comment_search:
                            filtered_comments = filtered_comments[filtered_comments['评论内容'].str.contains(comment_search, na=False)]
                        
                        # 根据情感得分筛选
                        if sentiment_filter != "全部" and '情感得分' in filtered_comments.columns:
                            if sentiment_filter == "积极":
                                filtered_comments = filtered_comments[filtered_comments['情感得分'] >= POSITIVE_THRESHOLD]
                            elif sentiment_filter == "消极":
                                filtered_comments = filtered_comments[filtered_comments['情感得分'] <= NEGATIVE_THRESHOLD]
                            else: # 中立
                                filtered_comments = filtered_comments[
                                    (filtered_comments['情感得分'] > NEGATIVE_THRESHOLD) & 
                                    (filtered_comments['情感得分'] < POSITIVE_THRESHOLD)
                                ]
                        
                        # 显示评论统计
                        st.markdown(f"**共找到 {len(filtered_comments)} 条评论（共 {len(comments_df)} 条）**")
                        
                        # 分页显示评论（每页20条）
                        page_size = 20
                        total_pages = (len(filtered_comments) + page_size - 1) // page_size
                        page = st.number_input("页码", min_value=1, max_value=total_pages, value=1)
                        start_idx = (page - 1) * page_size
                        end_idx = start_idx + page_size
                        page_comments = filtered_comments.iloc[start_idx:end_idx]
                        
                        # 显示评论表格
                        comment_display_cols = ['用户名', '用户城市', '评论内容', '点赞数', '评论时间']
                        if '情感得分' in page_comments.columns:
                            comment_display_cols.append('情感得分')
                        st.dataframe(
                            page_comments[comment_display_cols],
                            height=500,
                            use_container_width=True
                        )
                        
                        # 导出当前歌曲评论
                        if st.button("导出当前歌曲评论为CSV"):
                            export_path = DATA_DIR / f"{selected_song['歌曲名称']}_{selected_song['歌手']}_评论.csv"
                            # 清理文件名中的非法字符
                            export_path = Path(str(export_path).replace('/', '').replace('\\', '').replace('*', '').replace('?', '').replace('"', '').replace('<', '').replace('>', '').replace('|', ''))
                            comments_df.to_csv(export_path, index=False, encoding='utf-8-sig')
                            st.success(f"✅ 评论已导出至: {export_path}")
                    
                    except Exception as e:
                        st.error(f"加载评论失败: {str(e)}")
                else:
                    st.warning("该歌曲没有对应的评论文件或评论文件不存在")
        
        st.markdown("---")
        
        # 高级可视化
        if not filtered_df.empty:
            if selected_data_source == "13类风格歌单数据":
                plot_style_playlist_visualizations(filtered_df)
            else:
                plot_rank_comment_visualizations(filtered_df)
        else:
            st.warning("当前筛选条件下没有找到匹配的数据，无法生成可视化图表。")
        
        # 导出功能
        st.markdown("---")
        st.markdown('<div class="sub-title">💾 结果导出</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("导出筛选后数据为CSV文件"):
                if not filtered_df.empty:
                    export_path = DATA_DIR / f"筛选后的{load_summary['data_type']}数据.csv"
                    filtered_df.to_csv(export_path, index=False, encoding='utf-8-sig')
                    st.success(f"✅ CSV文件已导出至: {export_path}")
                else:
                    st.warning("❌ 没有可导出的数据。")
        
        with col2:
            if st.button("导出筛选后数据为Excel文件"):
                if not filtered_df.empty:
                    export_path = DATA_DIR / f"筛选后的{load_summary['data_type']}数据.xlsx"
                    filtered_df.to_excel(export_path, index=False, engine='openpyxl')
                    st.success(f"✅ Excel文件已导出至: {export_path}")
                else:
                    st.warning("❌ 没有可导出的数据。")
    else:
        st.error("无法显示数据分析和筛选功能，因为数据加载失败。")

# ---------------------- 运行入口 ----------------------
if __name__ == "__main__":
    main()