package util;

import org.apache.log4j.Logger;

import com.jcraft.jsch.UserInfo;
 
public class SUserInfo implements UserInfo {
 
	static final Logger logger = Logger.getLogger(SUserInfo.class);
    private String password;
    private String passPhrase;
 
    public SUserInfo (String password, String passPhrase) {
        this.password = password;
        this.passPhrase = passPhrase;
    }
 
    public String getPassphrase() {
        return passPhrase;
    }
 
    public String getPassword() {
        return password;
    }
 
    public boolean promptPassphrase(String arg0) {
        return true;
    }
 
    public boolean promptPassword(String arg0) {
        return false;
    }
 
    public boolean promptYesNo(String arg0) {
        return true;
    }
 
    public void showMessage(String arg0) {
        logger.info("SUserInfo.showMessage()");
    }
}
