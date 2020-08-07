<?php

class CsvStorageTest extends BaseTestClass
{
    protected function tearDown () : void {
        parent::tearDown();
        // delete the generated csv files
        $files = [
            __DIR__ . '/company.csv',
            __DIR__ . '/companies.csv',
            __DIR__ . '/members.csv',
        ];
        foreach ( $files as $file ) {
            if (file_exists($file)) {
                unlink($file);
            }
        }

        // reset states
        BaseCsvStorageProvider::$TYPE = '';
        BaseCsvStorageProvider::$HEADER_OFFSET = 0;
        BaseCsvStorageProvider::$SKIP_EMPTY = true;
        BaseCsvStorageProvider::$EXIT_ON_ERROR = true;
        BaseCsvStorageProvider::$STREAM_FILTERS = [];
        BaseCsvStorageProvider::$LIMIT = 20;
        BaseCsvStorageProvider::$USE_TRANSACTION = true;
        BaseCsvStorageProvider::$ENTRIES = [];
        BaseCsvStorageProvider::$FILTERS = [];
        BaseCsvStorageProvider::$HEADERS = [];
        BaseCsvStorageProvider::$MUTATION = [];
        BaseCsvStorageProvider::$CONNECTION = 'default';
        BaseCsvStorageProvider::$TABLE = '';
        BaseCsvStorageProvider::$FILENAME = '';
        BaseCsvStorageProvider::$DELIMITER = ',';
        BaseCsvStorageProvider::$EXCEPTION_RECEIVED = false;
    }

    protected function getCompanyProvider () {
        return new CompanyProvider($this->container);
    }

    protected function getMemberProvider () {
        return new Member($this->container);
    }

    protected function generateCompaniesData ($emptyLine = false, $filename = __DIR__ . '/companies.csv') {
        $data = <<<DATA
name,image_url,extra_data
Libero Morbi Accumsan Foundation,http://placehold.it/350x150,extra_data
Morbi Incorporated,http://placehold.it/350x150,extra_data
Imperdiet Limited,http://placehold.it/350x150,extra_data
Enim Sed Limited,http://placehold.it/350x150,extra_data

DATA;
        if ($emptyLine) {
            $data .= PHP_EOL;
        }
        $data .= <<<DATA
Leo Vivamus Consulting,http://placehold.it/350x150,extra_data
Feugiat Company,http://placehold.it/350x150,extra_data
Lobortis Consulting,http://placehold.it/350x150,extra_data
Nunc Pulvinar Incorporated,http://placehold.it/350x150,extra_data
Dolor Tempus Non PC,http://placehold.it/350x150,extra_data
Feugiat Tellus Lorem Company,http://placehold.it/350x150,extra_data
DATA;
        file_put_contents($filename, $data);
    }

    protected function generateMembersData ($emptyLine = false, $filename = __DIR__ . '/members.csv') {
        $data = <<<DATA
Audrey,35,Pede Nunc Sed Corporation,1-306-213-7650,lorem@duiCumsociis.co.uk
Dustin,21,Tristique Pharetra Institute,1-503-655-1646,Nulla.interdum.Curabitur@risus.com
Ann,40,Accumsan Interdum PC,1-376-771-7499,Morbi.sit@nonegestasa.com
Tanisha,20,Parturient Montes Nascetur Consulting,1-476-964-1349,ut@auctorMauris.org
Omar,43,Quisque Purus Foundation,1-676-331-5079,velit.Pellentesque.ultricies@purusmauris.edu

DATA;
        if ($emptyLine) {
            $data .= PHP_EOL;
        }
        $data .= <<<DATA
Madeline,25,Quis Arcu Inc.,1-543-583-8881,Aliquam.erat.volutpat@amet.com
Cole,36,Nunc Risus LLP,1-151-971-3050,Aliquam.tincidunt.nunc@acmattis.net
Julian,35,Est Nunc Associates,1-389-399-1089,nec.eleifend.non@orcitincidunt.com
Robin,39,Turpis Nulla LLP,1-882-935-4669,sit.amet@utpharetra.edu
Brandon,35,Augue Limited,1-189-777-6438,quam.Curabitur.vel@Integereu.com
DATA;
        file_put_contents($filename, $data);
    }

    public function testEventDispatcherIsWorking () {
        $this->addEventListener('test-true', function ($eventName) {
            return $eventName === 'test-true';
        });

        $this->addEventListener('test-count', function (...$payload) {
            return count($payload) === 3;
        });

        $this->assertTrue($this->dispatcher->until('test-true', 'test-true'));
        $this->assertTrue($this->dispatcher->until('test-count', [ 1, true, 'false' ]));
        $this->assertFalse($this->dispatcher->until('test-count'));
        $this->assertTrue(is_null($this->dispatcher->until('no-event-listener')));
    }

    public function testProvidingEventDispatcherFromOutside () {
        $company = $this->getCompanyProvider();
        $company->setEventDispatcher($this->dispatcher);
        // return false exists program early
        $this->addEventListener('dataset.reader.starting', function () {
            return false;
        });

        // call the import method to start execution
        $result = $company->import();

        $this->assertTrue(false === $result);
    }

    public function testCheckIfEventsAreFired () {
        // return false exists program early
        $this->addEventListener('dataset.reader.starting', function () {
            return false;
        });
        $received = false;

        // as returned false, it'll shoot exiting
        $this->addEventListener('dataset.reader.exiting', function () use (&$received) {
            $received = true;
        });

        // call the import method to start execution
        $result = $this->getCompanyProvider()->import();

        $this->assertTrue(false === $result);
        $this->assertTrue($received);
    }

    public function testRenamingEventNames () {
        // change the type of the event
        BaseCsvStorageProvider::$TYPE = 'reading-type';
        // return false exists program early
        $receivedNewType = false;
        $this->addEventListener('dataset.reading-type.starting', function () use (&$receivedNewType) {
            $receivedNewType = true;
        });

        $this->addEventListener('dataset.reading-type.preparing_reader', function () {
            return false;
        });

        // call the import method to start execution
        $result = $this->getCompanyProvider()->import();

        $this->assertTrue(false === $result);
        $this->assertTrue($receivedNewType);
    }
}