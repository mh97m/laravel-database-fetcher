# Duplicating MongoDB Data for Staging Environment in Laravel

The goal is to create a Laravel command that:

#### Connects to the production and staging databases.
#### Clears the staging database to ensure a fresh start.
#### Copies collections and their documents from the production to the staging database.
#### Replicates indexes to maintain query performance.

Its create a new database named : **env('DB_DATABASE').'_staging'**

###
###

## Installation

copy this command file to your project in this directory : 

#### App\Console\Commands

###
###

## Usage/Examples

```bash
php artisan database:fetch
```

###
###

## Acknowledgements

 - [How it Works. (medium)](https://medium.com/@mh97montazeri/duplicating-mongodb-data-for-staging-environment-in-laravel-816af3a21400)

