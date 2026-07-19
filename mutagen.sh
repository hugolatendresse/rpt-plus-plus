# run locally

cd ~/code/spy
mutagen daemon start
mutagen sync create \
	--name=spy-sync \
	--ignore-vcs \
	--ignore="node_modules" \
	--ignore="data" \
	--ignore="build" \
	--ignore=".venv" \
	--ignore="__pycache__" \
	. aws-ec2:/mnt/local_ssd/spy
