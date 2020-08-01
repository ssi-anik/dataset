<?php

use Dataset\DatabaseStorage;
use Illuminate\Database\Query\Expression;

class DbToCsv extends DatabaseStorage
{
    /*protected function raisedException (Throwable $t) : void {
        // log if required
        return;
    }*/

    /*protected function condition () : Closure {
        return function ($q) {
            $q->where('name', 'ILIKE', '%Audrey%');
        };
    }*/

    protected function filename () : string {
        return 'db-to-csv.csv';
    }

    protected function table () : string {
        return 'users';
    }

    protected function fileOpenMode () : string {
        return 'w';
    }

    protected function limit () : int {
        return 3;
    }

    protected function orderBy () : Expression {
        return $this->db()->raw('users.created_at');
    }

    protected function orderDirection () : string {
        return 'desc';
    }

    /*protected function exitOnError () : bool {
        return true;
    }*/

    protected function fetchUsing () : string {
        return 'cursor';
        // return 'chunk';
    }

    protected function columns () : array {
        // return [ '*' ];

        return [
            'phones.*',
            'emails.*',
            'users.*',
            $this->db()->raw('phones.id as p_id'),
            $this->db()->raw('users.id as u_id'),
            $this->db()->raw('emails.id as e_id'),
        ];
    }

    protected function mutation ($record) : array {
        return [
            'name-age'   => $record['name'] . '==' . $record['age'],
            'name-phone' => $record['name'] . '~~' . $record['number'],
        ];
    }

    /*protected function delimiterCharacter () : string {
        return ',';
    }*/

    /*protected function enclosureCharacter () : string {
        return '"';
    }*/

    /*protected function escapeCharacter () : string {
        return '\\';
    }*/

    protected function headers () : array {
        return [
            'u_id'       => "User's ID",
            'number'     => 'MSISDN',
            'name'       => 'name',
            'age'        => 'age',
            'created_at' => 'Date of Creation',
            'email'      => 'Email Address',
            'name-age'   => 'Name combined with age',
            'name-phone' => 'Name combined with phone',
        ];
    }

    public function joins () : array {
        return [
            [
                'table'  => 'emails',
                'first'  => 'emails.user_id',
                'second' => 'users.id',
                /*'type'     => 'inner',
                'operator' => '=',*/
            ],
            [
                'table'  => 'phones',
                'first'  => 'users.id',
                'second' => 'phones.user_id',
            ],
        ];
    }

    /*protected function getBuilder () : Illuminate\Database\Query\Builder {
        return $this->db()
                    ->table('users')
                    ->join('phones', 'phones.user_id', '=', 'users.id')
                    ->join('emails', 'emails.user_id', '=', 'users.id')
                    ->orderBy('users.id', 'desc')
                    ->where(function ($q) {
                        $q->where('name', 'ILIKE', '%Audrey%');
                    })
                    ->select([
                        'phones.*',
                        'emails.*',
                        'users.*',
                        $this->db()->raw('phones.id as p_id'),
                        $this->db()->raw('users.id as u_id'),
                        $this->db()->raw('emails.id as e_id'),
                    ]);
    }*/
}