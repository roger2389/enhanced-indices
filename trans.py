import nbformat
from nbconvert import MarkdownExporter
 
# 讀取 .ipynb 文件
with open('Simple Algorithm-TargetPercentPipeAlgo.ipynb', 'r', encoding='utf-8') as f:
    notebook_content = nbformat.read(f, as_version=4)
 
# 使用 MarkdownExporter 進行轉換
md_exporter = MarkdownExporter()
(body, resources) = md_exporter.from_notebook_node(notebook_content)
 
# 將結果寫入 .md 文件
with open('Simple Algorithm-TargetPercentPipeAlgo.md', 'w', encoding='utf-8') as f:
    f.write(body)