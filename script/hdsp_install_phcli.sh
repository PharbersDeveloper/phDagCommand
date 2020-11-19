kubectl get po -n hdsp | tail -n+2 | awk '{print $1}' | xargs -I {} kubectl exec -n hdsp -it {} -- pip3 install phcli==0.3.6
