## 説明
* テンプレートやサンプルデータテストファイルなどをおく

## アップロード

```
gcloud composer environments storage data import \
        --environment ${ENVIRONMENT_NAME} \
        --location ${ENVIRONMENT_LOCATION} \
        --source ./configs  


gcloud composer environments storage data import \
        --environment ${ENVIRONMENT_NAME} \
        --location ${ENVIRONMENT_LOCATION} \
        --source ./slack  


gcloud composer environments storage data import \
        --environment ${ENVIRONMENT_NAME} \
        --location ${ENVIRONMENT_LOCATION} \
        --source ./test  
```
