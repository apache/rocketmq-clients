# Transport

## List of Transport Header

<div align="center">

| Key                 | Description                     | Example                                                                                                                      |
| ------------------- | ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| authorization       | signature for authorization     | MQv2-HMAC-SHA1 Credential=J8gkX9OS3T6AdMHs, SignedHeaders=x-mq-date-time, Signature=B727846CAFB5B19E189D2C172CE2E1E7CDC0E7B7 |
| x-mq-date-time      | current timestamp               | 20210309T195445Z                                                                                                             |
| x-mq-client-id      | client unique identifier        | mbp@78774@2@3549a8wsr                                                                                                        |
| x-mq-request-id     | request id for each gRPC header | f122a1e0-dbcf-4ca4-9db7-221903354be7                                                                                         |
| x-mq-language       | language of client              | Java                                                                                                                         |
| x-mq-client-version | version of client               | 5.0.0                                                                                                                        |
| x-mq-protocol       | version of protocol             | v2                                                                                                                           |
