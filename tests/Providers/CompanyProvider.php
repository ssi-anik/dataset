<?php

class CompanyProvider extends BaseCsvStorageProvider
{
    protected function table () : string {
        return empty(static::$TABLE) ? 'companies' : static::$TABLE;
    }
}