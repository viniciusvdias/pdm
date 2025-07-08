# ny-taxis-big-data-analysis

ny-taxis-big-data-analisys

## How to execute the project

First of all, we need to get some data. You can either get the sample already in the data folder, but if you want more data, you can just execute the command bellow:

```sh
sh bin/download_data.sh <start_year>-<start_month> <final_year>-<final_month>
```

For exemple, to download all records from 2024, you type 'sh bin/download_data.sh 2024-01 2024-12'

Now we have the data, we can go to the code. We just need to build up a docker image by executing:

```sh
make
```

And then, finally, start up the container, by typing:

```sh
sh bin/start.sh
```

Obs: if you have any trouble while starting or executing the container, try to stop it (sh bin/stop.sh) and then remove it (sh bin/remove.sh) and finally, start up it again.

Now, we can be able to acess the jupyter browser enviroment with the directories data containing the data files and src containeing the notebook files.

## About the project

...
