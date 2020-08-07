from waitress import serve
import exportApi
serve(exportApi.app, host='0.0.0.0', port=5001)