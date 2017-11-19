## Python script accessing Twitters API for requesting user and tweet information

### Ignored Files
**credentials.py**
(used as long as there is no tokenRequest function serving the credentials from a database)
```python
# Application token/secret
APPLICATION = {
    "token": "",
    "secret": ""
}
# Users tokens/secrets
USERS = [
    {
        "token": "",
        "secret": ""
    },
    {
        "token":"",
        "secret":""
    }
]
```

**environments**

* [packages](./packages/) contains the used packages and uses python3 venv
* [requirements.txt](./requirements.txt) contains used packages in a simple list for a later use with docker 
