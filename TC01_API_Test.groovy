import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import com.kms.katalon.core.util.KeywordUtil
import com.kms.katalon.core.testobject.RequestObject
import com.kms.katalon.core.testobject.impl.HttpTextBodyContent

// ==========================================
// 1. KATALON SEBAGAI PRODUCER (POST DATA)
// ==========================================
KeywordUtil.logInfo("--- PRODUCER: Mengirim Data ke API ---")

// Membuat Request Object secara manual lewat code (Tanpa Object Repository UI)
RequestObject postRequest = new RequestObject('PostProducer')
postRequest.setRestUrl('https://reqres.in/api/users')
postRequest.setRequestMethod('POST')

// Menambahkan HTTP Body JSON
String jsonBody = '{"name": "Katalon Tester", "job": "QA Engineer"}'
postRequest.setBodyContent(new HttpTextBodyContent(jsonBody, "UTF-8", "application/json"))

// Eksekusi Request
def responseProducer = WS.sendRequest(postRequest)

// Validasi Response Producer
WS.verifyResponseStatusCode(responseProducer, 201)
WS.containsStringInResponse(responseProducer, 'Katalon Tester', false)


// ==========================================
// 2. KATALON SEBAGAI CONSUMER (GET DATA)
// ==========================================
KeywordUtil.logInfo("--- CONSUMER: Mengambil Data dari API ---")

RequestObject getRequest = new RequestObject('GetConsumer')
getRequest.setRestUrl('https://reqres.in/api/users/2')
getRequest.setRequestMethod('GET')

// Eksekusi Request
def responseConsumer = WS.sendRequest(getRequest)

// Validasi Response Consumer
WS.verifyResponseStatusCode(responseConsumer, 200)
WS.verifyElementPropertyValue(responseConsumer, 'data.first_name', 'Janet')

KeywordUtil.logInfo("--- Semua pengujian API berhasil di-automasi! ---")