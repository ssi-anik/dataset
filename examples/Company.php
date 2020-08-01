<?php

use Dataset\CsvStorage;

class Company extends CsvStorage
{
    /*protected function raisedException (Throwable $t) : void {
        // log here if required
        return;
    }*/

    /*protected function exitOnError () : bool {
        return true;
    }*/

    /*protected function table () : string {
        return 'companies';
    }*/

    protected function filterInput (array $record) : array {
        /*return [
            'name'      => $record['name'],
            'image_url' => $record['image_url'],
            'slug'      => $record['slug'],
        ];*/
        unset($record['extra_data']);

        return $record;
    }

    /*protected function useTransaction () : bool {
        // return false;
        return true;
    }*/

    protected function filename () : string {
        return 'companies.csv';
    }

    protected function fileOpenMode () : string {
        return 'r';
    }

    protected function streamFilters () : array {
        return [];
        /*return [
            'string.toupper',
        ];*/
    }

    protected function headerOffset () : ?int {
        return 0;
        // return null;
    }

    protected function mutation (array $record) : array {
        return [
            'slug' => str_replace(' ', '-', strtolower($record['name'])),
        ];
    }

    /*protected function entries () : array {
        return [
            // if your table is different, use that table name. "companies" is an example
            'companies' => function (Model $model, array $record, array $previous) {
                // $previous contains other model objects
                // $previous['companies']->id will return id of inserted company
                // if used form the second index
                // have to maintain the dependency
                $model->name = $record['name'];
                $model->image_url = $record['image_url'];
                $model->slug = $record['slug'];
                $model->save();

                return $model;
            },
        ];
    }*/

    /*protected function headers () : array {
        return [
            'name',
            'picture',
            'extra_data',
        ];
    }*/
}