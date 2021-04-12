# composer-test

### Cloud Composerへ環境を用意

```
# environment name
# location name
# create or update
export MODULE_PATH=./modules
export SQL_PATH=./sqls

export ENVIRONMENT_NAME=your-workflow-environment-name
export ENVIRONMENT_LOCATION=asia-northeast1
export SETUP_TYPE=create
export PROJECT_ID=your_project_id
export ENVIRONMENT_LOCATION=asia-northeast1

# 環境を作成する
gcloud composer environments ${SETUP_TYPE} ${ENVIRONMENT_NAME} \
      --location asia-northeast1

unset ENVIRONMENT_NAME
unset ENVIRONMENT_LOCATION
unset SETUP_TYPE
unset PROJECT_ID
unset ENVIRONMENT_LOCATION
unset MODULE_PATH
unset SQL_PATH
```

### SQLを生成する

### タスクを作成する


### DAGをアップロード

```
gcloud composer environments storage dags import \
        --environment ${ENVIRONMENT_NAME} \
        --location ${ENVIRONMENT_LOCATION} \
        --source ./TT_TEST
```

### module uploat

```
gcloud composer environments storage plugins import \
        --environment ${ENVIRONMENT_NAME} \
        --location ${ENVIRONMENT_LOCATION} \
        --source ./modules  
```

### SQLアップロード

```
gcloud composer environments storage data import \
        --environment ${ENVIRONMENT_NAME} \
        --location ${ENVIRONMENT_LOCATION} \
        --source ./sql       
```
