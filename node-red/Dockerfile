FROM nodered/node-red:4.1.17-18

USER root
RUN apk add --no-cache python3 make g++
USER node-red

# node-red-contrib-opcua pulls in node-opcua, whose transitive deps can
# need a native build step on Alpine - hence python3/make/g++ above.
#
# node-opcua-alarm-condition's bundled "hexy" resolves to an ESM-only
# release, which breaks node-opcua-debug's require() of it and silently
# kills registration of every node-red-contrib-opcua node (including
# OpcUa-Server). Pin it back to the last CommonJS release.
RUN npm pkg set "overrides.hexy=0.3.5"
RUN npm install --no-audit --no-update-notifier --omit=dev node-red-contrib-opcua@0.2.355

EXPOSE 1880 4840
