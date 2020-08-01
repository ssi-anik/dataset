<?php

use Dataset\CsvStorage;
use Illuminate\Database\Eloquent\Model;

class User extends CsvStorage
{
    protected function useTransaction () : bool {
        return false;
    }

    protected function filename () : string {
        return 'user_data_file.csv';
    }

    protected function fileOpenMode () : string {
        return 'r+';
    }

    /*protected function streamFilters () : array {
        // check for filters: https://csv.thephpleague.com/9.0/connections/filters/
        return [
            'string.toupper',
        ];
    }*/

    protected function headerOffset () : ?int {
        return null;
    }

    protected function entries () : array {
        return [
            'users'  => function (Model $model, array $record, array $previous) {
                $model->name = $record['name'];
                $model->age = $record['age'];
                $model->created_at = date('Y-m-d H:i:s');
                $model->updated_at = date('Y-m-d H:i:s');
                $model->save();

                return $model;
            },
            'phones' => function (Model $model, array $record, array $previous) {
                $model->user_id = $previous['users']->id;
                $model->number = $record['msisdn'];
                $model->save();

                return $model;
            },
            'emails' => function (Model $model, array $record, array $previous) {
                $model->user_id = $previous['users']->id;
                $model->msisdn_id = $previous['phones']->id;
                $model->email = $record['EMAIL'];
                $model->save();

                return $model;
            },
        ];
    }

    protected function headers () : array {
        return [
            'name',
            'age',
            'company',
            'msisdn',
            'EMAIL',
        ];
    }
}