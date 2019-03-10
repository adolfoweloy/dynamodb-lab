# Project created to study dynamodb

I've created this small project just to study DynamoDB and run some experiences.
It does not provides anything valuable other than some DynamoDB API usage examples.

## Investigating dynamoDB by practicing

The following command shows exactly the amount of consumed capacity units for read operation
using an eventual consistent read (which consumes 0.5 RCUs): 

```
aws dynamodb get-item \
--table-name Music \
--key '{"Artist":{"S": "Hoodoo Gurus"}, "Song":{"S": "1000 Miles Away"}}' \
--return-consumed-capacity TOTAL
```
