SELECT
  *
FROM `your-project-id.dataset_name.table_name`

WHERE
  dt BETWEEN @from_date AND @end_date
