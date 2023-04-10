# Simple ORM

## Setup

    pip install -r requirements.txt

> Create `.env` file for connecting to PostgreSQL Database and run `source .env` see `.example.env`

- [ ] DB_HOST
- [ ] DB_PORT
- [ ] DB_NAME
- [ ] DB_USER
- [ ] DB_PASSWORD

## Tasks Completed

- [x] Create DB tables based on class definitions
- [x] Create table rows by instantiating a class and calling `save` method
- [x] Filter (SELECT) data based on exact matching
- [x] Update existing rows
- [x] Prevent users from entering wrong field names
- [x] Handle at least 3 field types including CharField and IntegerField
- [x] Validate field types
- [x] Filter the result of an already filtered data
- [x] Support lazy query evaluation (refer to the example below)
- [x] Handle foreign keys. (Relational DB)
- [ ] Automatically detects schema changes (migrations)
- [ ] Be used across multiple threads without causing unnecessary blocking


## To Run

- Create tables (like in examples directory) & migrate
- Then you can query table using the `objects` Manager i.e `Users.objects.filter(name="test")` etc.
