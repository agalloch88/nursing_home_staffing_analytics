source .venv/bin/activate
export AWS_PROFILE=default AWS_REGION=us-east-1
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export LAKE=nh-staffing-lake-$ACCOUNT
echo "account $ACCOUNT, bucket $LAKE"
