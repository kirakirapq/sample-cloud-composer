## 説明
* モジュールとSQL置き場

## 利用方法

```
import sys
from airflow.models import Variable
sys.path.append(Variable.get('module_path'))

from functions import get_dag, get_bq_operator, slack_failure_notification, slack_success_notification
```

## アップロード

```
gcloud composer environments storage plugins import \
        --environment ${ENVIRONMENT_NAME} \
        --location ${ENVIRONMENT_LOCATION} \
        --source ./modules  


gcloud composer environments storage plugins import \
        --environment ${ENVIRONMENT_NAME} \
        --location ${ENVIRONMENT_LOCATION} \
        --source ./sqls  
```
