<?php

class CompanyProvider extends BaseCsvStorageProvider
{
    protected function table () : string {
        return 'company';
    }
}