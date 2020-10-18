import * as glue from '@aws-cdk/aws-glue';

export const TWEETS_TABLE_PARTITION_KEYS = [
    {
      name: 'year',
      type: glue.Schema.STRING
    },
    {
      name: 'month',
      type: glue.Schema.STRING
    },
    {
      name: 'day',
      type: glue.Schema.STRING
    },
    {
      name: 'hour',
      type: glue.Schema.STRING
    },
  ]
  
  export const TWEETS_TABLE_COLUMNS = [
    {
      name: 'id',
      type: glue.Schema.BIG_INT,
    },
    {
      name: 'created_at',
      type: glue.Schema.TIMESTAMP,
    },
    {
      name: 'full_text',
      type: glue.Schema.STRING,
    },
    {
      name: 'lang',
      type: glue.Schema.STRING,
    },
  ]