#include "stdafx.h"

//#if _MSC_VER && _MSC_VER < 1600
//#pragma warning(disable: 4996)
//#endif

// DA: Driver to fairport / pstsdk to read and parse a pst file
// Much faster than using Outlook/VBA directly.
// 

#include <iostream>
#include <iomanip>
#include <stdio.h>

#include <boost/system/config.hpp>
#include <boost/asio.hpp>

#include "fairport/pst.h"

using boost::asio::ip::tcp;
using namespace fairport;
using namespace std;
using namespace std::tr1::placeholders;

std::string Sanitise(const std::string & in)
{
	// Removes control characters, and encloses raw string in CDATA tags
	std::string out("<![CDATA[");
	for(size_t i=0;i<in.length();++i)
	{
		char wcIn=in[i];
		if(wcIn==']'&&i<in.length()-1){
			if(in[i+1]=='>')
			{
				// avoid ]]> - create second CDATA section after second ]
				out+="]]]><!CDATA[>";
				++i;
			} else
			{
				out+="]";
			}
		} else
		if((wcIn>8&&wcIn<14&&wcIn!=12)||(wcIn>31&&wcIn<128))
		{
			// Preserve 
			out+=wcIn;
		}
	}
	out=out+"]]>";
	return out;
}

class MessageProcessor {
private:
	std::string m_strHost; // eg:localhost
	std::string m_strPort; // eg:8984	
	std::string m_strPST; // path to PST
	std::string m_strdgpreamble;
	std::string m_strmsgpreamble;

	unsigned long m_sentBytes;
	unsigned long m_nProcessed; // Number processed
	unsigned long m_nSuccess;
	unsigned long m_nFail;
	unsigned long m_nAttachments;
	bool m_bStripAttachments;
	bool m_bSubmitToSearch;

	static std::string CleanString(const std::string & in) 
	{
		// Removes control characters, and encloses raw string in CDATA tags
		std::string out("<![CDATA[");
		for(size_t i=0;i<in.length();++i)
		{
			char wcIn=in[i];
			if(wcIn==']'&&i<in.length()-1)
			{
				if(in[i+1]=='>')
				{
					// avoid ]> - create another CDATA section after ']'
					out+="]]]><!CDATA[>"; // = ']' + close cdata + open cdata + '>'
					++i;
				} 
				else
				{
					out+="]";
				}
			} 
			else if((wcIn>8&&wcIn<14&&wcIn!=12)||(wcIn>31&&wcIn<128))
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
				// File with the name of current attachment already exits.
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
		std::ofstream binFile(ss.str().c_str(), std::ios::out | std::ios::binary);
		binFile << attch;
		binFile.close();
	}

	bool SubmitMessage(const std::ostringstream& strBodyStrm, const std::string& strID)
	{
		// Submits a well-formed message to Solr service
		boost::asio::io_service io_service;
		boost::system::error_code error = boost::asio::error::host_not_found;
		tcp::resolver resolver(io_service);
		tcp::resolver::query query(m_strHost, m_strPort);
		tcp::resolver::iterator endpoint_iterator = resolver.resolve(query, error);
		if (error) {
			std::cerr << error.message() << endl;
			m_nFail++;
			return false;
		}

		// Use endpoint.
		tcp::socket socket(io_service);
		boost::asio::connect(socket, endpoint_iterator,error);
		if (error) {
			std::cerr << error.message() << endl;
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
		request_stream << "Connection: close\r\n\r\n";
		request_stream << strBody;

		// Send request.
		size_t nBytes = boost::asio::write(socket, request, error);

		if (error) {
			std::cerr << "Send error: " << error.message() << endl;
			m_nFail++;
			return false;
		}
		m_sentBytes+=nBytes;
		// Read the response status line. The response streambuf will automatically
		// grow to accommodate the entire line. The growth may be limited by passing
		// a maximum size to the streambuf constructor.
		boost::asio::streambuf response;
		boost::asio::read_until(socket, response, "\r\n", error);
		if (error) {
			std::cerr << "Response error: " << error.message() << endl;
			m_nFail++;
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
			std::cerr << "Invalid response" << endl;
			m_nFail++;
			return false;
		}

		// Read the response headers, which are terminated by a blank line.
		if (status_code != 200)
		{
			std::cerr << "Response returned with status code " << status_code << endl;
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

			std::cerr << endl << "Msg ID : " << strID << endl;
			m_nFail++;
			return false;
		}
		m_nSuccess++;
		return true;
	}

public:
	MessageProcessor(const std::string& pst, const std::string& host, const std::string& port, const std::string& url, const std::string& timeout_ms, bool bDoIndex=false,bool bDoAttachments=false):
		m_strPST(pst), 
		m_strHost(host),
		m_strPort(port),
		m_bSubmitToSearch(bDoIndex),
		m_bStripAttachments(bDoAttachments),
		m_sentBytes(0),
		m_nSuccess(0),
		m_nFail(0),
		m_nAttachments(0)
		{
		m_strdgpreamble=
			  	"POST " + url + " HTTP/1.1\r\n" + 
				"Host: " + m_strHost + ":" + m_strPort + "\r\n" +
				"Content-Type: text/xml\r\n";
		m_strmsgpreamble="<add commitWithin=\"" + timeout_ms + "\"><doc><field name=\"id\">";
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
					ostrID << std::hex << std::uppercase << setw(2) << setfill('0') << long(vID[i]);
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
				<< std::dec << setw(4) << 1900+tmDate.tm_year << "-" << setw(2) << setfill('0') << 1+tmDate.tm_mon << "-" // 0-11, need to add one
				<< setw(2) << setfill('0') << tmDate.tm_mday << "T"	<< setw(2) << setfill('0') << tmDate.tm_hour << ":"
				<< setw(2) << setfill('0') << tmDate.tm_min << ":" << setw(2) << setfill('0') << tmDate.tm_sec << "Z";

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
				if(strSubj.size() && strSubj[0] == message_subject_prefix_lead_byte) strSubj=strSubj.substr(2);
				strSubj=CleanString(strSubj);
			}
			ostrOut << "</field><field name=\"subject\">" << strSubj;

			// Full set of recipients
			ostrOut << "</field><field name=\"to\"><![CDATA[";
			for(message::recipient_iterator ri=m.recipient_begin();ri!=m.recipient_end();++ri)
			{
				ostrOut << ri->get_property_row().read_prop<std::string>(0x3001) << " ;"; // get_name()
			}

			// Number of attachments
			size_t nAttach=m.get_attachment_count();
			// InterlockedExchangeAdd?
			m_nAttachments+=nAttach;
			ostrOut << "]]></field><field name=\"attachments\">" << nAttach;

			// Filenames of attachments (where possible)
			std::string strAttach;
			if(nAttach==0)
			{
				strAttach="None";
			}
			else
			{		
				int i=1;
				for(message::attachment_iterator ai=m.attachment_begin();ai!=m.attachment_end();++ai)
				{
					std::string strFilename;
					if (ai->is_message())
					{
						fairport::message msg=ai->open_as_message();
						// Mostly this is null, but occasionally not. For uniformity, keep consistent naming
						strFilename=strID + ":att(" + std::to_string((_Longlong)i) + ")";
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
			ostrOut << "</field></doc></add>" << ends;
			m_nProcessed++;
			// Send to server
			return (m_bSubmitToSearch?SubmitMessage(ostrOut,strID):true);
		}
		catch(key_not_found<prop_id>&a)
		{
			std::cerr << "Key not found: 0x" << std::hex << long(a.which()) << std::dec << "\t\t" << "Msg ID was:" << strID << endl;
		}
		catch(...)
		{
			std::cout << "General error! Msg ID was:" << strID << endl;
		}
		return false;
	}

};

bool SubmitMessage(const std::ostringstream& strBodyStrm, const std::string& strID)
{
	std::string m_strURL="localhost";
	std::string m_strPort="8984"; 

	// Get a list of endpoints corresponding to the server name.
	boost::asio::io_service io_service;
	boost::system::error_code error = boost::asio::error::host_not_found;

	tcp::resolver resolver(io_service);
	tcp::resolver::query query(m_strURL, m_strPort);
	tcp::resolver::iterator endpoint_iterator = resolver.resolve(query, error);
    if (error) {
        std::cerr << error.message() << endl;
        return false;
    }

	// Use endpoint
	tcp::socket socket(io_service);
	boost::asio::connect(socket, endpoint_iterator,error);
	if (error) {
        std::cerr << error.message() << endl;
        return false;
    }

	// Form the request. We specify the "Connection: close" header so that the
	// server will close the socket after transmitting the response. This will
	// allow us to treat all data up until the EOF as the content.
	boost::asio::streambuf request;
	std::ostream request_stream(&request);	

	request_stream << "POST /solr/PstSearch/update HTTP/1.1\r\n"; //PstSearch
	request_stream << "Host: " << m_strURL << ":" << m_strPort << "\r\n";
	request_stream << "Content-Type: text/xml\r\n";

	std::string strBody = strBodyStrm.str();
	strBody.resize(strBody.length()-1);
	request_stream << "Content-Length: " << strBody.length() << "\r\n";
	request_stream << "Connection: close\r\n\r\n";
	request_stream << strBody;

	// Send the request.
	size_t nBytes = boost::asio::write(socket, request, error);

	if (error) {
        std::cout << "Send error: " << error.message() << endl;	
        return false;
    }

	// Read the response status line. The response streambuf will automatically
	// grow to accommodate the entire line. The growth may be limited by passing
	// a maximum size to the streambuf constructor.
	boost::asio::streambuf response;
	boost::asio::read_until(socket, response, "\r\n", error);
	if (error) {
        std::cout << "Response error: " << error.message() << endl;
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
		std::cerr << "Invalid response" << endl;
		return false;
	}

	// Read the response headers, which are terminated by a blank line.
	if (status_code != 200)
	{
		std::cerr << "Response returned with status code " << status_code << endl;
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

		std::cerr << endl << "Msg ID : " << strID << endl;

		return false;
	}
	return true;
}

void SaveAttachment(const fairport::attachment& attch, const std::string& strFileName)
{
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
    bool done = false;
    unsigned int i = 1;
    std::stringstream ss;
    ss << strFileName;
    do
    {
        // Check if the file already exists
        std::ifstream testFile(ss.str().c_str(), std::ios::in | std::ios::binary);
        if(testFile.is_open())
        {
            testFile.close();
            // File with the name of current attachment already exits.
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
    //std::cout << "Saving image attachment to '" << ss.str() << "'\n";
    std::ofstream binFile(ss.str().c_str(), std::ios::out | std::ios::binary);
    binFile << attch;
    binFile.close();
}

// Process an entire message
bool ProcessMessage(const fairport::message& m, const std::string & path, const int m_nCommitWithin, const std::string& attachmentid="", tm*creationtm=0)
{ 
	bool m_bSaveAttachments=true;
	std::ostringstream ostrOut, ostrID;
	std::string strID;
	try {
		ostrOut << "<add commitWithin=\"" << m_nCommitWithin << "\"><doc><field name=\"id\">";
		if(attachmentid!="")
		{
			strID=attachmentid;
		} else
		{
			// Entry ID as string, Standard Outlook format suitable for direct use for lookup:
			std::vector<unsigned char> vID=m.get_entry_id();	

			for(size_t i=0;i<vID.size();i++){
				ostrID << std::hex << std::uppercase << setw(2) << setfill('0') << long(vID[i]);
			}
			strID=ostrID.str();
		}
		// Parent PST file
		ostrOut << strID << "</field><field name=\"pstfile\">" << path;

		// Occasionally in messages as attachments the creation time and sender don't exist; 
		// for these cases we use the parent's creation time and an empty sender

		// Creation time, as YYYY-MM-DDTHH:mm:ssZ datetime 
		// get_property_bag().read_prop<boost::posix_time::ptime>(0x0e06)
		tm tmDate;
		if(attachmentid!=""&&!m.get_property_bag().prop_exists(0x0e06))
			tmDate=*creationtm;
		else
			tmDate=to_tm(m.get_delivery_time());
		ostrOut << "</field><field name=\"created\">"
			<< std::dec << setw(4) << 1900+tmDate.tm_year << "-" 
			<< setw(2) << setfill('0') << 1+tmDate.tm_mon << "-" // 0-11, need to add one
			<< setw(2) << setfill('0') << tmDate.tm_mday << "T"
			<< setw(2) << setfill('0') << tmDate.tm_hour << ":"
			<< setw(2) << setfill('0') << tmDate.tm_min << ":"
			<< setw(2) << setfill('0') << tmDate.tm_sec << "Z";

		// Display name of sender. Rather than amend Fairport, I've looked up relevant property ID (0x0C1a) here
		std::string strSender;
		if(attachmentid!=""&&!m.get_property_bag().prop_exists(0x0C1A))
			strSender="Missing";
		else
			strSender=Sanitise(m.get_property_bag().read_prop<std::string>(0x0C1A));
		ostrOut << "</field><field name=\"sender\">" << strSender;

		// Subject, as ASCII string (or "Empty")
		std::string strSubj="Empty";
		if(m.has_subject())
		{
			strSubj=m.get_property_bag().read_prop<std::string>(0x37);
			if(strSubj.size() && strSubj[0] == message_subject_prefix_lead_byte) strSubj=strSubj.substr(2);
			strSubj=Sanitise(strSubj);
		}
		ostrOut << "</field><field name=\"subject\">" << strSubj;

		// Full set of recipients
		ostrOut << "</field><field name=\"to\"><![CDATA[";
		for(message::recipient_iterator ri=m.recipient_begin();ri!=m.recipient_end();++ri)
		{
			ostrOut << ri->get_property_row().read_prop<std::string>(0x3001) << " ;"; // get_name()
		}

		// Number of attachments
		size_t nAttach=m.get_attachment_count();
		ostrOut << "]]></field><field name=\"attachments\">" << nAttach;

		// Filenames of attachments where possible
		std::string strAttach;
		if(nAttach==0)
		{
			strAttach="None";
		}else{		
			int i=1;
			for(message::attachment_iterator ai=m.attachment_begin();ai!=m.attachment_end();++ai)
			{
				std::string strFilename;
				if (ai->is_message())
				{

					fairport::message msg=ai->open_as_message();
					// Mostly this is null, but occasionally not. For uniformity, keep consistent naming
					//if(strFilename=="Null")
					strFilename=strID + ":att(" + std::to_string((_Longlong)i) + ")";
					//std::cout << "Message attachment filename: " << strFilename << endl;
					ProcessMessage(msg,path,m_nCommitWithin,strFilename,&tmDate);
				}else
				{
					// This is rather nasty as it's using exception handling for control flow
					if(ai->get_property_bag().prop_exists(0x3707))
						strFilename=ai->get_property_bag().read_prop<std::string>(0x3707);
					else if(ai->get_property_bag().prop_exists(0x3704))
						strFilename=ai->get_property_bag().read_prop<std::string>(0x3704);
					else
						strFilename="Null";
					if (strFilename!="Null"&&m_bSaveAttachments)
						SaveAttachment(*ai,strFilename);
				}
				strAttach+=strFilename;
				strAttach+=" ;";
				i++;
			}
		}
		ostrOut << "</field><field name=\"filenames\">" << Sanitise(strAttach);

		// IPM class of message. 
		ostrOut << "</field><field name=\"class\">" << m.get_property_bag().read_prop<std::string>(0x001A);

		// ASCII string of body text, or "Empty" if not available
		std::string strBody = "Empty";
		if (m.has_body()) strBody = Sanitise(m.get_property_bag().read_prop<std::string>(0x1000));
		if(strBody.length()>32767) strBody="TRUNCATED";
		ostrOut << "</field><field name=\"body\">" << strBody;
		ostrOut << "</field></doc></add>" << ends;
		// Send to server
		return SubmitMessage(ostrOut,strID);
		//std::cout<<ostrOut.str()<<endl;
	}

    catch(key_not_found<prop_id>&a)
    {
        std::cerr << "Key not found: 0x" << std::hex << long(a.which()) << std::dec << endl << "Msg ID:" << strID << endl;
    }
	catch(...)
	{
		std::cout << "General error! (msg id:" << strID << ")";
	}
	return false;
}

void ProcessFolder(const fairport::folder& f,const std::string& path,std::string name)
{
	name = name + ":" + f.get_property_bag().read_prop<std::string>(0x3001);
    std::cout << name << " (" << f.get_message_count() << ")\n";
	int i=0,j=0;
	time_t stime, etime;
	time(&stime);
	MessageProcessor mp(path,"localhost","8984","/solr/PstSearch/update","60000",false,false);
	for(fairport::folder::message_iterator mi=f.message_begin();mi!=f.message_end();++mi)
	{
		//if(ProcessMessage(*mi,path,20000))j++;
		if(mp.ProcessMessage(*mi))j++;
		i++;
		if(i%100==0)std::cout<<i << " messages processed\t\t\r";
	}
	time(&etime);
	std::cout << j << " of " << i << " messages successfully processed in " << (etime-stime) << " seconds\t\t" << endl;
    for(fairport::folder::folder_iterator subf = f.sub_folder_begin(); subf != f.sub_folder_end(); ++subf)
    {
        ProcessFolder(*subf, path, name);
    }

}

int main(int, char** argv)
{
    std::string path(argv[1]);
    std::wstring wpath(path.begin(), path.end());
    fairport::pst store(wpath);
	char szPath[2048];
	_fullpath(szPath,path.c_str(),2048);
	std::string fPath(szPath);
	std::string strPST = store.get_property_bag().read_prop<std::string>(0x3001);
	std::cout << "Processing PST: " << strPST << " (file: " <<fPath << ")" << endl;
	ProcessFolder(store.open_root_folder(),fPath,strPST);
}
