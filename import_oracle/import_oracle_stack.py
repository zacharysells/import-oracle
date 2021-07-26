from aws_cdk import (
    core, 
    aws_ec2 as ec2,
    aws_rds as rds
)


class ImportOracleStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        vpc = ec2.Vpc(self, "vpc", cidr="10.0.0.0/16", max_azs=2)
        rds_instance = rds.DatabaseInstance(
            self, "rds",
            database_name="ORCL",
            credentials=rds.Credentials.from_password(self.node.try_get_context("oracle-rds-username"), core.SecretValue(self.node.try_get_context("oracle-rds-password"))),
            engine=rds.DatabaseInstanceEngine.ORACLE_SE2,
            license_model=rds.LicenseModel.LICENSE_INCLUDED,
            vpc=vpc,
            deletion_protection=False,
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.BURSTABLE3, 
                ec2.InstanceSize.SMALL,
            ),
            vpc_placement=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC)
        )
        rds_instance.connections.allow_from(ec2.Peer.ipv4(self.node.try_get_context("public-ipv4-cidr-allowed")), ec2.Port.tcp(1521), "Allow oracle db port connection from Home IP")
        
