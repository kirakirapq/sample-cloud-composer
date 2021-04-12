## 説明
* airflow.models.Variable

## 使い方
 1. Airflow UI > Admin > Variables　へデータをインポート
 2. 実装コードから呼び出し

```
from airflow.models import Variable

value = Variable.get("key")
```
