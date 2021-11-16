    Unicode True
    !include "MUI.nsh"

    SetCompressor /SOLID lzma
    Name "H2"
    Icon "favicon.ico"
    OutFile "../../../h2web/h2-setup.exe"
    CRCCheck on

    InstallDir "$PROGRAMFILES\H2"
    InstallDirRegKey HKCU "Software\H2" ""
    RequestExecutionLevel highest

;--------------------------------
;Variables

    Var MUI_TEMP
    Var STARTMENU_FOLDER

;--------------------------------
;Interface Settings

    !define MUI_ABORTWARNING

;--------------------------------
;Language Selection Dialog Settings

    ;Remember the installer language
    !define MUI_LANGDLL_REGISTRY_ROOT "HKCU"
    !define MUI_LANGDLL_REGISTRY_KEY "Software\H2"
    !define MUI_LANGDLL_REGISTRY_VALUENAME "Installer Language"

;--------------------------------
;Pages

    !insertmacro MUI_PAGE_DIRECTORY

    ;Start Menu Folder Page Configuration
    !define MUI_STARTMENUPAGE_REGISTRY_ROOT "HKCU"
    !define MUI_STARTMENUPAGE_REGISTRY_KEY "Software\H2"
    !define MUI_STARTMENUPAGE_REGISTRY_VALUENAME "Start Menu Folder"
    !define MUI_FINISHPAGE_SHOWREADME "$INSTDIR\docs\index.html"

    !insertmacro MUI_PAGE_STARTMENU Application $STARTMENU_FOLDER
    !insertmacro MUI_PAGE_INSTFILES
    !insertmacro MUI_PAGE_FINISH

    !insertmacro MUI_UNPAGE_CONFIRM
    !insertmacro MUI_UNPAGE_INSTFILES

;--------------------------------
;Languages

    !insertmacro MUI_LANGUAGE "English" # first language is the default language
    !insertmacro MUI_LANGUAGE "German"
    !insertmacro MUI_LANGUAGE "Spanish"
    !insertmacro MUI_LANGUAGE "French"
    !insertmacro MUI_LANGUAGE "SimpChinese"
    !insertmacro MUI_LANGUAGE "TradChinese"
    !insertmacro MUI_LANGUAGE "Japanese"
    !insertmacro MUI_LANGUAGE "Korean"
    !insertmacro MUI_LANGUAGE "Italian"
    !insertmacro MUI_LANGUAGE "Dutch"
    !insertmacro MUI_LANGUAGE "Danish"
    !insertmacro MUI_LANGUAGE "Swedish"
    !insertmacro MUI_LANGUAGE "Norwegian"
    !insertmacro MUI_LANGUAGE "Finnish"
    !insertmacro MUI_LANGUAGE "Greek"
    !insertmacro MUI_LANGUAGE "Russian"
    !insertmacro MUI_LANGUAGE "Portuguese"
    !insertmacro MUI_LANGUAGE "PortugueseBR"
    !insertmacro MUI_LANGUAGE "Polish"
    !insertmacro MUI_LANGUAGE "Ukrainian"
    !insertmacro MUI_LANGUAGE "Czech"
    !insertmacro MUI_LANGUAGE "Slovak"
    !insertmacro MUI_LANGUAGE "Croatian"
    !insertmacro MUI_LANGUAGE "Bulgarian"
    !insertmacro MUI_LANGUAGE "Hungarian"
    !insertmacro MUI_LANGUAGE "Romanian"
    !insertmacro MUI_LANGUAGE "Latvian"
    !insertmacro MUI_LANGUAGE "Macedonian"
    !insertmacro MUI_LANGUAGE "Estonian"
    !insertmacro MUI_LANGUAGE "Turkish"
    !insertmacro MUI_LANGUAGE "Lithuanian"
    !insertmacro MUI_LANGUAGE "Catalan"
    !insertmacro MUI_LANGUAGE "Slovenian"
    !insertmacro MUI_LANGUAGE "Serbian"
    !insertmacro MUI_LANGUAGE "SerbianLatin"
    !insertmacro MUI_LANGUAGE "Arabic"
    !insertmacro MUI_LANGUAGE "Farsi"
    !insertmacro MUI_LANGUAGE "Hebrew"
    !insertmacro MUI_LANGUAGE "Indonesian"
    !insertmacro MUI_LANGUAGE "Mongolian"
    !insertmacro MUI_LANGUAGE "Luxembourgish"
    !insertmacro MUI_LANGUAGE "Albanian"
    !insertmacro MUI_LANGUAGE "Breton"
    !insertmacro MUI_LANGUAGE "Belarusian"
    !insertmacro MUI_LANGUAGE "Icelandic"
    !insertmacro MUI_LANGUAGE "Malay"
    !insertmacro MUI_LANGUAGE "Bosnian"
    !insertmacro MUI_LANGUAGE "Kurdish"

;--------------------------------
;Reserve Files

    ;These files should be inserted before other files in the data block
    ;Keep these lines before any File command
    ;Only for solid compression (by default, solid compression is enabled for BZIP2 and LZMA)

    !insertmacro MUI_RESERVEFILE_LANGDLL

;--------------------------------
;Installer Sections

Section "All"

    SetOutPath "$INSTDIR\src"
    File /r /x CVS /x .cvsignore /x .svn ..\..\src\*.*
    SetOutPath "$INSTDIR\bin"
    File /x CVS /x .cvsignore ..\..\bin\h2*
    SetOutPath "$INSTDIR\docs"
    File /r /x CVS /x .cvsignore /x .jar ..\..\docs\*.*
    SetOutPath "$INSTDIR\service"
    File /r /x CVS /x .cvsignore /x .svn ..\..\service\*.*
    SetOutPath "$INSTDIR"
    File /r /x CVS /x .cvsignore ..\..\build.bat
    File /r /x CVS /x .cvsignore ..\..\build.sh

    WriteRegStr HKCU "Software\H2" "" $INSTDIR
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\H2" "DisplayName" "H2"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\H2" "UninstallString" "$INSTDIR\Uninstall.exe"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\H2" "InstallLocation" "$INSTDIR"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\H2" "DisplayIcon" "$INSTDIR\src\installer\favicon.ico"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\H2" "NoModify" "1"
    WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\H2" "NoRepair" "1"

    WriteUninstaller "$INSTDIR\Uninstall.exe"

    !insertmacro MUI_STARTMENU_WRITE_BEGIN Application
    SetOutPath "$INSTDIR\bin"
    CreateDirectory "$SMPROGRAMS\$STARTMENU_FOLDER"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\H2 Console.lnk" "cmd" "/c h2w.bat" "$INSTDIR\src\installer\favicon.ico" 0 SW_SHOWMINIMIZED "" "Start the Console"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\H2 Console (Command Line).lnk" "cmd" "/c h2.bat" "$INSTDIR\src\installer\favicon.ico" 0 SW_SHOWMINIMIZED "" "Start the Console from command line (using h2.bat)"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\H2 Documentation.lnk" "$INSTDIR\docs\index.html"
;    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Uninstall.lnk" "$INSTDIR\Uninstall.exe"
    !insertmacro MUI_STARTMENU_WRITE_END

SectionEnd

;--------------------------------
;Uninstaller Section

Section "Uninstall"

    SetOutPath "$INSTDIR\.."
    RMDir /r "$INSTDIR"
    Delete "$INSTDIR\Uninstall.exe"

    !insertmacro MUI_STARTMENU_GETFOLDER Application $MUI_TEMP

    Delete "$SMPROGRAMS\$MUI_TEMP\*.lnk"

    ;Delete empty start menu parent diretories
    StrCpy $MUI_TEMP "$SMPROGRAMS\$MUI_TEMP"

    startMenuDeleteLoop:
        ClearErrors
        RMDir $MUI_TEMP
        GetFullPathName $MUI_TEMP "$MUI_TEMP\.."

        IfErrors startMenuDeleteLoopDone

        StrCmp $MUI_TEMP $SMPROGRAMS startMenuDeleteLoopDone startMenuDeleteLoop
    startMenuDeleteLoopDone:

    DeleteRegKey HKCU "Software\H2"
    DeleteRegKey HKCU "Software\Microsoft\Windows\CurrentVersion\Uninstall\H2"

SectionEnd
