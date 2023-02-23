pipeline {
    agent {label 'linux'}
    stages {
        stage('Deploy connector') {
            steps {
                sh('cd /var/lib/jenkins/ && ./deploy-connector.sh')
            }
        }
    }
}