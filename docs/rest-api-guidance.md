# GET METHODS - Reusuable API Response Structure


## Structure when returning a list of dictionaries (GET /users)
```json
{
  "success": True,
  "data": [
    {
      "user_id": "123",
      "upn": "user1@example.com",
      "display_name": "User 1",
      "email": "user1@example.com",
      "phone": "1234567890",
      "address": "123 Main St, Anytown, USA",
      "city": "Anytown",
      "state": "CA"
    },
    {
      "user_id": "123",
      "upn": "user1@example.com",
      "display_name": "User 1",
      "email": "user1@example.com",
      "phone": "1234567890",
      "address": "123 Main St, Anytown, USA",
      "city": "Anytown",
      "state": "CA"
    },
  ],
   "metadata": {
    "tenant_id": "123",
    "tenant_name": "Tenant 1",

  },
   "actions": [],
}
```


## structure when returning an aggregated metric or calcuation (GET /users/analysis)
```json
{
  "success": True,
  "data": [],
  "metadata": {
    "tenant_id": "123",
    "tenant_name": "Tenant 1",
    "calc1": 100,
    "calc2": 10,

  },
  "actions": [
  {
    "title": "Action 1",
    "description": "Action 1 description",
    "action": "disable"
  },
  {
    "title": "Action 2",
    "description": "Action 2 description",
    "action": "disable"
  },
  ],
}
```