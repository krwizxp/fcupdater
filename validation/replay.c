#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static char *response(const char *path,size_t *n) {
 const char *text=strstr(path,"NetFunnel")?"NetFunnel.gRtype=5002;NetFunnel.gControl.result='5002:200:key=training-ticket&nwait=0&ttl=1';":strstr(path,"main_download_excel")?NULL:"opinet_key.value = 'training-key';";
 if(text){*n=strlen(text);char *p=malloc(*n);memcpy(p,text,*n);return p;}
 FILE *f=fopen(getenv("PGO_XLS"),"rb");if(!f)abort();fseek(f,0,SEEK_END);*n=(size_t)ftell(f);rewind(f);char *p=malloc(*n);if(fread(p,1,*n,f)!=*n)abort();fclose(f);return p;
}
#ifdef _WIN32
#include <windows.h>
#define API __declspec(dllexport)
typedef struct {char *body;size_t size,position;} Fake;
static int scenario(void){const char*s=getenv("PGO_SCENARIO");return s?atoi(s):0;}
API void *WINAPI WinHttpOpen(const wchar_t*a,DWORD b,const wchar_t*c,const wchar_t*d,DWORD e){return calloc(1,sizeof(Fake));}
API void *WINAPI WinHttpConnect(void*a,const wchar_t*b,WORD c,DWORD d){return calloc(1,sizeof(Fake));}
API void *WINAPI WinHttpOpenRequest(void*a,const wchar_t*b,const wchar_t*c,const wchar_t*d,const wchar_t*e,const wchar_t**f,DWORD g){char path[4096];size_t i=0;for(;c[i]&&i<4095;i++)path[i]=(char)c[i];path[i]=0;Fake*h=calloc(1,sizeof(Fake));h->body=response(path,&h->size);return h;}
API BOOL WINAPI WinHttpCloseHandle(void*v){Fake*h=v;if(h){free(h->body);free(h);}return TRUE;}
API BOOL WINAPI WinHttpSetOption(void*a,DWORD b,void*c,DWORD d){return TRUE;}
API BOOL WINAPI WinHttpSetTimeouts(void*a,int b,int c,int d,int e){return TRUE;}
API BOOL WINAPI WinHttpSendRequest(void*a,const wchar_t*b,DWORD c,void*d,DWORD e,DWORD f,ULONG_PTR g){return TRUE;}
API BOOL WINAPI WinHttpReceiveResponse(void*a,void*b){if(scenario()==5)Sleep(61000);return TRUE;}
API BOOL WINAPI WinHttpQueryHeaders(void*v,DWORD q,const wchar_t*n,void*out,DWORD*size,DWORD*index){Fake*h=v;DWORD type=q&0xffff;if(type==19){*(DWORD*)out=200;*size=4;return TRUE;}if(type==5 && index && *index==0){*(DWORD*)out=(DWORD)h->size;*size=4;if(scenario()!=1)*index=1;return TRUE;}if(type==43){if(scenario()==2)return TRUE;if(scenario()==3){*size=3;SetLastError(122);return FALSE;}if(scenario()==4){if(!out){*size=8;SetLastError(122);return FALSE;}memcpy(out,L"a=b",8);*size=8;return TRUE;}}SetLastError(12150);return FALSE;}
API BOOL WINAPI WinHttpReadData(void*v,void*out,DWORD cap,DWORD*read){Fake*h=v;size_t n=h->size-h->position;if(n>cap)n=cap;if(n)memcpy(out,h->body+h->position,n);h->position+=n;*read=(DWORD)n;return TRUE;}
#else
#include <stdarg.h>
#define CURL_DISABLE_TYPECHECK
#include <curl/curl.h>
static char url[4096];static curl_write_callback body_cb,header_cb;static void *body_data,*header_data;
void curl_easy_reset(CURL*h){url[0]=0;body_cb=header_cb=NULL;body_data=header_data=NULL;}
CURLcode curl_easy_setopt(CURL*h,CURLoption o,...){va_list a;va_start(a,o);if(o==CURLOPT_URL){snprintf(url,sizeof(url),"%s",va_arg(a,char*));}else if(o==CURLOPT_WRITEFUNCTION){body_cb=va_arg(a,curl_write_callback);}else if(o==CURLOPT_HEADERFUNCTION){header_cb=va_arg(a,curl_write_callback);}else if(o==CURLOPT_WRITEDATA){body_data=va_arg(a,void*);}else if(o==CURLOPT_HEADERDATA){header_data=va_arg(a,void*);}va_end(a);return CURLE_OK;}
CURLcode curl_easy_getinfo(CURL*h,CURLINFO q,...){va_list a;va_start(a,q);if(q==CURLINFO_RESPONSE_CODE)*va_arg(a,long*)=200;else if(q==CURLINFO_SCHEME)*va_arg(a,char**)="https";else abort();va_end(a);return CURLE_OK;}
CURLcode curl_easy_perform(CURL*h){size_t n;char *body=response(url,&n);char header[128];snprintf(header,sizeof(header),"HTTP/1.1 200 OK\r\n");if(header_cb(header,1,strlen(header),header_data)!=strlen(header))return CURLE_WRITE_ERROR;snprintf(header,sizeof(header),"Content-Length: %zu\r\n",n);if(header_cb(header,1,strlen(header),header_data)!=strlen(header))return CURLE_WRITE_ERROR;if(header_cb("\r\n",1,2,header_data)!=2)return CURLE_WRITE_ERROR;for(size_t pos=0;pos<n;){size_t len=n-pos;if(len>16384)len=16384;if(body_cb(body+pos,1,len,body_data)!=len){free(body);return CURLE_WRITE_ERROR;}pos+=len;}free(body);return CURLE_OK;}
#endif
