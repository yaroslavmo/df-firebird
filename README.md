# df-firebird
Firebird SQL Database support for DreamFactory.


## Note: 
This library requires the **interbase** PHP extension to be installed.

    sudo apt-get install php7.0-interbase
    
This library has following dependencies.
    
    "dreamfactory/df-core":                 "~0.10.0",
    "jacquestvanzuydam/laravel-firebird":   "dev-5.4-support as dev-master"
    "doctrine/dbal":                        "2.5.*",

It extends the *doctrine/dbal* library for Firebird support based on [this](https://github.com/helicon-os/doctrine-dbal/tree/DBAL-95-firebird) repository.
    
   