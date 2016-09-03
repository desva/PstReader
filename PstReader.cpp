#include "stdafx.h"

// Driver to fairport / pstsdk to read and parse a pst file
// Much faster than using Outlook/VBA directly.
// 
// Optionally allows for extracting attachments and saving directly to disk

#include <iostream>
#include <iomanip>
#include <stdio.h>
#include <regex>
#include <io.h>

#define BOOST_DATE_TIME_NO_LIB 
#define BOOST_REGEX_NO_LIB 

#include <boost/asio.hpp>
#include "fairport/pst.h"

#pragma comment( compiler )

class CTimer{
	__int64	m_ticksStart;
	__int64	m_ticks;
	static __int64 m_freq; // Adjusted so as to return time in microseconds
public:
	CTimer():m_ticksStart(0),m_ticks(0)
	{
		if(!m_freq)
		{
			QueryPerformanceFrequency((LARGE_INTEGER*)&m_freq);
			m_freq/=1000000;
		}
	};
	// Maximum is 2^32/1000000=2^32/~2^20=2^12 seconds or a few minutes over an hour....
	const unsigned long MicroSeconds() const 
	{
		return (const unsigned long)(m_ticks/m_freq);
	};	
	const std::string Seconds() const
	{
		long ms = MicroSeconds();
		long div=ms/1000000;
		long rem=ms%1000000;
		char szBuff[2*_MAX_INT_DIG];
		sprintf_s(szBuff, sizeof(szBuff), "%d.%d", div,rem);
		return (std::string(szBuff));
	}
	void Start() 
	{
		QueryPerformanceCounter((LARGE_INTEGER*)&m_ticksStart);
	};
	void Mark() 
	{
		QueryPerformanceCounter((LARGE_INTEGER *)&m_ticks);
		__int64 ticks=m_ticks;
		m_ticks-=m_ticksStart;
		m_ticksStart=ticks;
	};
};

__int64 CTimer::m_freq=0;

class CPSTProcessor {
private:
	std::string m_strHost; // eg:localhost
	std::string m_strPort; // eg:8984	
	std::string m_strPST; // path to PST
	std::string m_strdgpreamble;
	std::string m_strmsgpreamble;
	std::regex m_rxFolder;
	std::regex m_rxExtn;
	// Options
	bool m_bDoExtRE;
	bool m_bDoFolderRE;
	bool m_bDoFireForget;
	// Statistics
	unsigned long m_sentBytes;
	unsigned long m_nProcessed; // Number processed sucessfully
	unsigned long m_nProcFail; // Number which failed to process due to missing keys etc..
	unsigned long m_nSuccess;
	unsigned long m_nFail;
	unsigned long m_nAttachments;
	unsigned long m_nAttSaved; // Saved
	unsigned long m_nAttFailed; // failed to save
	unsigned long m_nMsgAttachment;
	unsigned long m_nFolders;

	bool m_bStripAttachments;
	bool m_bSubmitToSearch;

	static std::string CleanString(const std::string & in) 
	{
		// Removes control characters, and encloses raw string in CDATA tags
		std::string out("<![CDATA[");
		for(size_t i=0;i<in.length();++i)
		{
			char wcIn=in[i];
			if(wcIn==']'&&i<in.length()-2)
			{
				if(in[i+1]==']'&&in[i+2]=='>')
				{
					// avoid ]]> 
					out+="]]&gt;";
					i+=2;
				} 
				else
				{
					out+="]";
				}
			} 
			else if((wcIn>8&&wcIn<14&&wcIn!=11&&wcIn!=12)||(wcIn>31&&wcIn<128))
			{
				// Preserve 
				out+=wcIn;
			} // else ignore character
		}
		out=out+"]]>";
		return out;
	}
	void SaveAttachment(const fairport::attachment& attch, const std::string& strFileName)
	{
		// Save attachement to file - assumes not a message
		size_t pos = strFileName.rfind(".");
		std::string strBase, strExtn;
		if(pos==std::string::npos)
		{
			strBase=strFileName;
			strExtn="";
		}
		else
		{
			strBase=strFileName.substr(0, pos);
			strExtn=strFileName.substr(pos);
		}
		if(m_bDoExtRE)
		{
			if(strExtn=="")return; // nothing to compare against
			std::string strFilter=strExtn.substr(1); // skip dot
			//std::transform(strFilter.begin(), strFilter.end(), strFilter.begin(), tolower);
			if(!regex_search(strFilter,m_rxExtn))return;
			//if(m_strExtensions.find(strFilter)==std::string::npos)return;
		}
		if(attch.content_size()==0)
		{
			m_nAttFailed++;
			return;
		}
		bool done = false;
		unsigned int i=1;
		std::stringstream ss;
		ss << strFileName;
		do
		{
			// Check if the file already exists
			std::ifstream testFile(ss.str().c_str(), std::ios::in|std::ios::binary);
			if(testFile.is_open())
			{
				testFile.close();
				// File with the name of current attachment already exists.
				// Do not overwrite the existing file, instead use a different name
				// for this file.
				ss.str(std::string());
				ss.clear();
				ss << strBase;
				ss << "(" << i++ << ")";
				ss << strExtn;
			}
			else
			{
				done = true;
			}
		} while(!done);
		// Save attachment
		if(attch.get_property_bag().prop_exists(0x3701))
		{
			std::ofstream binFile(ss.str().c_str(), std::ios::out | std::ios::binary);
			binFile << attch;
			binFile.close();
			m_nAttSaved++;
		}
		else
		{
			std::cout << "Failed to save: " << strFileName << std::endl;
			m_nAttFailed++;
		}
	}
	bool SubmitMessage(const std::ostringstream& strBodyStrm, const std::string& strID)
	{
		using boost::asio::ip::tcp;
		// Submits a well-formed message to Solr service
		boost::asio::io_service io_service;
		boost::system::error_code error = boost::asio::error::host_not_found;
		tcp::resolver resolver(io_service);
		tcp::resolver::query query(m_strHost, m_strPort);
		tcp::resolver::iterator endpoint_iterator = resolver.resolve(query, error);
		tcp::resolver::iterator end;
		if (error) {
			std::cerr << error.message() << std::endl;
			m_nFail++;
			return false;
		}

		// Use endpoint.
		tcp::socket socket(io_service);
		error = boost::asio::error::host_not_found;
		while (error && endpoint_iterator != end)
	    {
			socket.close();
			boost::asio::connect(socket, endpoint_iterator,error);
			//socket.connect(*endpoint_iterator++, error);
		}
		//boost::asio::connect(socket, endpoint_iterator,error);
		if (error) {
			std::cerr << error.message() << std::endl;
			m_nFail++;
			return false;
		}

		// Form request.
		boost::asio::streambuf request;
		std::ostream request_stream(&request);	
		request_stream << m_strdgpreamble;
		std::string strBody = strBodyStrm.str();
		strBody.resize(strBody.length()-1); // strip terminating null
		request_stream << "Content-Length: " << strBody.length() << "\r\n";
		request_stream << (m_bDoFireForget?"Connection: keep-alive\r\n":"Connection: close\r\n");		
		request_stream << "\r\n" << strBody;

		// Send request.
		size_t nBytes = boost::asio::write(socket, request, error);

		if (error) {
			std::cerr << "Send error: " << error.message() << std::endl;
			m_nFail++;
			socket.close();
			return false;
		}
		m_sentBytes+=nBytes;
		
		if(m_bDoFireForget)
		{
			m_nSuccess++;
			return true;
		}

		// Read the response status line. The response streambuf will automatically
		// grow to accommodate the entire line. The growth may be limited by passing
		// a maximum size to the streambuf constructor.
		boost::asio::streambuf response;
		boost::asio::read_until(socket, response, "\r\n", error);
		if (error) {
			std::cerr << "Response error: " << error.message() << std::endl;
			m_nFail++;
			socket.close();
			return false;
		}

		// Check that response is OK.
		std::istream response_stream(&response);
		std::string http_version;
		response_stream >> http_version;
		unsigned int status_code;
		response_stream >> status_code;
		std::string status_message;
		std::getline(response_stream, status_message);

		if (!response_stream || http_version.substr(0, 5) != "HTTP/")
		{
			std::cerr << "Invalid response" << std::endl;
			m_nFail++;
			socket.close();
			return false;
		}

		// Read the response headers, which are terminated by a blank line.
		if (status_code != 200)
		{
			std::cerr << "Response returned with status code " << status_code << std::endl;
			boost::asio::read_until(socket, response, "\r\n\r\n");
			// Process the response headers.
			std::string header;
			while (std::getline(response_stream, header) && header != "\r")
				std::cerr << header << "\n";
			std::cerr << "\n";

			// Write whatever content we already have to output.
			if (response.size() > 0)
				std::cerr << &response;

			// Read until EOF, writing data to output as we go.
			while (boost::asio::read(socket, response,boost::asio::transfer_at_least(1), error))
				std::cerr << &response;

			std::cerr << std::endl << "Msg ID : " << strID << std::endl;
			std::cerr << strBody << std::endl;
			m_nFail++;
			socket.close();
			return false;
		}
		m_nSuccess++;
		socket.close();
		return true;
	}
	void CommitMessages()
	{
		if(m_bSubmitToSearch)
		{
			std::ostringstream ostrOut;
			ostrOut << "<commit/> ";
			SubmitMessage(ostrOut,"Null");
		}
	}
public:
	CPSTProcessor(const std::string& pst, const std::string& host, const std::string& port, const std::string& path, const std::string& timeout_ms, bool bDoIndex=false,bool bDoAttachments=false,const std::string& ext="", const std::string& fld="", bool bDoFireForget=false):
		m_strPST(pst), m_strHost(host), m_strPort(port),
		m_bSubmitToSearch(bDoIndex), m_bStripAttachments(bDoAttachments),
		m_bDoExtRE(false),m_bDoFolderRE(false),m_bDoFireForget(bDoFireForget),
		m_sentBytes(0), 
		m_nProcessed(0), m_nProcFail(0), m_nSuccess(0), m_nFail(0),
		m_nAttachments(0), m_nMsgAttachment(0),m_nAttSaved(0),m_nAttFailed(0),
		m_nFolders(0)
		{
		m_strdgpreamble=
			  	"POST " + path + " HTTP/1.1\r\n" + 
				"Host: " + m_strHost + ":" + m_strPort + "\r\n" +
				"Content-Type: text/xml\r\n";
		m_strmsgpreamble="<add commitWithin=\"" + timeout_ms + "\"><doc><field name=\"id\">";
		
		if(ext!="")
			{
			m_rxExtn=std::regex(ext.c_str(),
				std::regex_constants::ECMAScript|std::regex_constants::icase|std::regex_constants::nosubs|std::regex_constants::optimize);
			m_bDoExtRE=true;
			}
		if(fld!="")
			{
			m_rxFolder=std::regex(fld.c_str(),
				std::regex_constants::ECMAScript|std::regex_constants::nosubs|std::regex_constants::optimize);
			m_bDoFolderRE=true;
			}
		}
	bool ProcessMessage(const fairport::message& m, const std::string& attachmentid="", tm*creationtm=0)
	{
		// Process an entire message
		// Rather than amend Fairport, in most cases I've used the relevant property ID where a native Fairport accessor doesn't exist
		std::ostringstream ostrOut, ostrID;
		std::string strID;
		try {
			ostrOut << m_strmsgpreamble;
			if(attachmentid!="")
			{
				strID=attachmentid;
			}
			else
			{
				// Entry ID as string, Standard Outlook format suitable for direct use for lookup:
				std::vector<unsigned char> vID=m.get_entry_id();	
				for(size_t i=0;i<vID.size();i++)
					ostrID << std::hex << std::uppercase << std::setw(2) << std::setfill('0') << long(vID[i]);
				strID=ostrID.str();
			}
			// Parent PST file
			ostrOut << strID << "</field><field name=\"pstfile\">" << m_strPST;

			// Occasionally in messages as attachments the creation time and sender don't exist.
			// For these cases we use the parent's creation time and an empty sender
			tm tmDate;
			if(attachmentid!=""&&!m.get_property_bag().prop_exists(0x0e06))
				tmDate=*creationtm;
			else
				tmDate=to_tm(m.get_delivery_time());
			// Creation time, as YYYY-MM-DDTHH:mm:ssZ datetime 
			ostrOut << "</field><field name=\"created\">"
				<< std::dec << std::setw(4) << 1900+tmDate.tm_year << "-" << std::setw(2) << std::setfill('0') << 1+tmDate.tm_mon << "-" // 0-11, need to add one
				<< std::setw(2) << std::setfill('0') << tmDate.tm_mday << "T" << std::setw(2) << std::setfill('0') << tmDate.tm_hour << ":"
				<< std::setw(2) << std::setfill('0') << tmDate.tm_min << ":" << std::setw(2) << std::setfill('0') << tmDate.tm_sec << "Z";

			// Display name of sender.
			std::string strSender;
			if(attachmentid!=""&&!m.get_property_bag().prop_exists(0x0C1A))
				strSender="Missing";
			else
				strSender=CleanString(m.get_property_bag().read_prop<std::string>(0x0C1A));
			ostrOut << "</field><field name=\"sender\">" << strSender;

			// Subject, as ASCII string (or "Empty")
			std::string strSubj="Empty";
			if(m.has_subject())
			{
				strSubj=m.get_property_bag().read_prop<std::string>(0x37);
				if(strSubj.size() && strSubj[0] == fairport::message_subject_prefix_lead_byte) strSubj=strSubj.substr(2);
				strSubj=CleanString(strSubj);
			}
			ostrOut << "</field><field name=\"subject\">" << strSubj;

			// Full set of recipients
			std::string strRecipients;
			ostrOut << "</field><field name=\"to\">";
			for(fairport::message::recipient_iterator ri=m.recipient_begin();ri!=m.recipient_end();++ri)
			{
				strRecipients+=ri->get_property_row().read_prop<std::string>(0x3001);
				strRecipients+=" ;"; // get_name()
			}
			ostrOut << CleanString(strRecipients); // occasionally contain non-XML compliant characters

			// Number of attachments
			size_t nAttach=m.get_attachment_count();
			m_nAttachments+=nAttach;
			ostrOut << "</field><field name=\"attachments\">" << nAttach;

			// Filenames of attachments (where possible)
			std::string strAttach;
			if(nAttach==0)
			{
				strAttach="None";
			}
			else
			{		
				int i=1;
				for(fairport::message::attachment_iterator ai=m.attachment_begin();ai!=m.attachment_end();++ai)
				{
					std::string strFilename;
					if (ai->is_message())
					{
						m_nMsgAttachment++;
						fairport::message msg=ai->open_as_message();
						// Mostly this is null, but occasionally not. For uniformity, keep consistent naming
						strFilename=strID + ".att(" + std::to_string((_Longlong)i) + ")";
						ProcessMessage(msg,strFilename,&tmDate);
					}
					else
					{
						if(ai->get_property_bag().prop_exists(0x3707))
							strFilename=ai->get_property_bag().read_prop<std::string>(0x3707);
						else if(ai->get_property_bag().prop_exists(0x3704))
							strFilename=ai->get_property_bag().read_prop<std::string>(0x3704);
						else
							strFilename="Null";
						if (strFilename!="Null"&&m_bStripAttachments)
							SaveAttachment(*ai,strID+"."+strFilename);
					}
					strAttach+=strFilename;
					strAttach+=" ;";
					i++;
				}
			}
			ostrOut << "</field><field name=\"filenames\">" << CleanString(strAttach);

			// IPM class of message. 
			ostrOut << "</field><field name=\"class\">" << m.get_property_bag().read_prop<std::string>(0x001A);

			// ASCII string of body text, or "Empty" if not available
			std::string strBody = "Empty";
			if (m.has_body()) strBody = CleanString(m.get_property_bag().read_prop<std::string>(0x1000));
			//if(strBody.length()>32767) strBody="TRUNCATED";
			ostrOut << "</field><field name=\"body\">" << strBody;
			ostrOut << "</field></doc></add>" << std::ends;
			m_nProcessed++;
			// Send to server
			return (m_bSubmitToSearch?SubmitMessage(ostrOut,strID):true);
		}
		catch(fairport::key_not_found<fairport::prop_id>&a)
		{
			std::cerr << "Key not found: 0x" << std::hex << long(a.which()) << std::dec << "\t\t" << "Msg ID was:" << strID << std::endl;
			m_nProcFail++;
		}
		catch(...)
		{
			std::cerr << "General error! Msg ID was:" << strID << std::endl;
			m_nProcFail++;
		}
		return false;
	}
	void ProcessFolder(const fairport::folder& f,const std::string& path,std::string name,int indent)
	{
		m_nFolders++;
		std::string strFolder=f.get_property_bag().read_prop<std::string>(0x3001); // Folder name
		if(strFolder!="")name = name + "/" + strFolder;
		int k=indent;
		std::string strIndent;
		while(--k)strIndent+="  ";
		std::cout << strIndent << name << " (" << f.get_message_count() << " items)\n";
		if(!m_bDoFolderRE||std::regex_search(strFolder,m_rxFolder))
		{
			int i=0,j=0;
			CTimer oTimer;
			oTimer.Start();
			for(fairport::folder::message_iterator mi=f.message_begin();mi!=f.message_end();++mi)
			{
				if(ProcessMessage(*mi))j++;
				i++;
				if(i%100==0)
				{
					std::cout << strIndent << i << " messages processed\t\t\r";
				}
			}
			CommitMessages();
			oTimer.Mark();
			std::cout << strIndent << j << " (of " << i << ") messages successfully processed in " << oTimer.Seconds() << " seconds\t\t" << std::endl;
		}
		for(fairport::folder::folder_iterator subf = f.sub_folder_begin(); subf != f.sub_folder_end(); ++subf)
		{
			ProcessFolder(*subf, path, name, indent+1);
		}
	}
	void ProcessPst(bool bShowStats)
	{
		std::wstring wpath(m_strPST.begin(), m_strPST.end());
		fairport::pst store(wpath);
		char szPath[2048];
		_fullpath(szPath,m_strPST.c_str(),2048);
		std::string fPath(szPath);
		std::string strPST = store.get_property_bag().read_prop<std::string>(0x3001);
		std::cout << "Processing PST: " << strPST << " (file: " <<fPath << ")" << std::endl;
		CTimer oTimer;
		oTimer.Start();
		ProcessFolder(store.open_root_folder(),fPath,strPST,1);
		oTimer.Mark();
		if(bShowStats)
		{
			std::cout << std::endl << std::endl
				<< "Total time: " << oTimer.Seconds() << " seconds" << std::endl
				<< "Folders traversed: " << m_nFolders << std::endl
				<< "Messages sucessfully processed (of which are embedded attachments): " << m_nProcessed << " (" << m_nMsgAttachment << ")" << std::endl
				<< "Messages failed to process: " << m_nProcFail << std::endl
				<< "Successfully submitted: " << (m_nSuccess==0?0:m_nSuccess-m_nFolders) << std::endl // Each folder comes with a "commit" submission
				<< "Failed to submit: " << m_nFail << std::endl
				<< "Bytes sent: " << m_sentBytes << std::endl
				<< "Attachments processed (saved/failed to save): " << m_nAttachments << " (" << m_nAttSaved << "/" << m_nAttFailed << ")" << std::endl << std::endl;
		}
	}
};

static void Usage(char*szName)
{
	std::string strAppName(szName);
	std::transform(strAppName.begin(), strAppName.end(), strAppName.begin(), tolower);
	std::cout 
		<< std::endl
		<< "Usage :" << std::endl
		<< "\t" << strAppName << " [/Z] [/A[:ext]] [/F:folder] [/S] [URL] pstfile.pst" << std::endl << std::endl 
		<< "where :" << std::endl 
		<< "\tpstfile.pst is pst file to process." << std::endl
		<< "\tURL is optional fully qualified Solr URL of the form:\n\t  http://hostname:port/update_url" << std::endl
		<< "\tfor example:\n\t  http://localhost:8984/solr/PstSearch/update ." << std::endl
		<< "\tOptional command /Z to stream updates (ignores server responses - faster but doesn't validate);" << std::endl 
		<< "\tOptional command /A indicates to strip attachments [file extension .ext only];" << std::endl 
		<< "\toptional command /F indicates to process only folders matching folder;" << std::endl
		<< "\toptional switch /S indicates to show summary statistics after processing each pst." << std::endl 
		<< "\tBoth /A and /F take regular expressions as patterns to match," <<std::endl
		<< "\tcomplex patterns should be enclosed in quotes." <<std::endl << std::endl;
	exit(EXIT_SUCCESS);
}

int main(int argc, char** argv)
{
	system("cls");
	// Program name for usage
	char*szProgName=strrchr(argv[0],'\\');
	if(szProgName==0)
	{
		szProgName=argv[0];
	}
	else {
		szProgName++; // skip "\"
	}
	if(argc<2)Usage(szProgName);
	std::string strHost("null"),strUrlPath("null"),strPort("null");
	std::string strExtensions(""),strPath(""),strFolder("");

	// Parse command line
	bool bDoAttachments(false),bDoSolr(false),bShowStats(false),bDoFireForget(false);
	while(--argc)
	{
		std::string strArg(argv[argc]);
		std::string strArgOrig(strArg);
		std::transform(strArg.begin(), strArg.end(), strArg.begin(), tolower);
		if(strArg.find("http://")==0)
		{
			// Parse out host, path, port
			int i=strArg.find(":",7);
			if(i==std::string::npos)
			{
				std::cout << "Must specify port." << std::endl;
				Usage(szProgName);
			}
			strHost=strArg.substr(7,i-7);
			if(strHost.length()<3)
			{
				std::cout << "Must specify hostname." << std::endl;
				Usage(szProgName);
			}
			std::cout << "Host: " << strHost << std::endl;
			int ii=strArg.find("/",i);
			if(ii==std::string::npos)
			{
				std::cout << "Must specify path (or ""/"" for none)" << std::endl;
				Usage(szProgName);
			}
			strPort=strArg.substr(i+1,ii-i-1);
			std::cout << "Port: " << strPort << std::endl;
			strUrlPath=strArgOrig.substr(ii);
			std::cout << "Path: " << strUrlPath << std::endl;
			bDoSolr=true;
		}
		else if(strArg.find("/a")==0||strArg.find("-a")==0)
		{
			bDoAttachments=true;
			std::cout << "Extension(s) specified: ";
			if(strArg.length()>3&&strArg[2]==':')
			{
				strExtensions=strArgOrig.substr(3);
				std::cout << strExtensions << std::endl;
			} 
			else
			{ 
				std::cout << "*" << std::endl;
			}
		}
		else if(strArg.find("/s")==0||strArg.find("-s")==0)
		{
			bShowStats=true;
		}
		else if(strArg.find("/z")==0||strArg.find("-z")==0)
		{
			bDoFireForget=true;
		}
		else if(strArg.find("/f:")==0||strArg.find("-f:")==0)
		{
			strFolder=strArgOrig.substr(3);
			std::cout << "Folders specified: " << strArgOrig.substr(3) << std::endl;
		}
		else
		{
			// PST file
			strPath=strArg;
		}
	}
	if(strPath=="")
	{
		std::cout << "PST file not specified." << std::endl;
		Usage(szProgName);
	}

	// Globbing for pst files
	intptr_t file;
	_finddata_t filedata;
	size_t pos = strPath.rfind('\\');
	std::string strPathPart="";
	if(pos!=std::string::npos)strPathPart=strPath.substr(0, pos+1);
	file = _findfirst(strPath.c_str(),&filedata);
	if(file!=-1)
	{
		do
		{
			strPath=strPathPart + filedata.name;
			//std::cout << strPath << std::endl;
			FILE *fp;
			int err = fopen_s(&fp, strPath.c_str(), "rb");
			if(err==0)
			{
				fclose(fp);
				// OK
				//pstFile.close();
				CPSTProcessor pp(strPath,strHost,strPort,strUrlPath,"60000",bDoSolr,bDoAttachments,strExtensions,strFolder,bDoFireForget);
				//"localhost","8984","/solr/PstSearch/update","60000",bDoSolr,bDoAttachments);
				try{
					pp.ProcessPst(bShowStats);
				}
				catch(...)
				{
					std::cout << "An unhandled error occured :-(" << std::endl;
				}
			}
			else
			{
				std::cout << "Unable to open PST file: " << strPath << std::endl;
			}
		} while (_findnext(file,&filedata) == 0);
	}

	exit(EXIT_SUCCESS);
}
