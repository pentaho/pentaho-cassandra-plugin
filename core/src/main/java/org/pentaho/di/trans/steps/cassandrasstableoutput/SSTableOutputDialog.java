/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2022 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.cassandrasstableoutput;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.events.dialog.FilterType;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterFileDialogTextVar;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterOptions;
import org.pentaho.di.ui.core.events.dialog.SelectionOperation;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;


/**
 * Dialog class for the SSTableOutput step
 *
 * @author Rob Turner (robert{[at]}robertturner{[dot]}com{[dot]}au)
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class SSTableOutputDialog extends BaseStepDialog implements StepDialogInterface {

  private static final Class<?> PKG = SSTableOutputMeta.class;

  private final SSTableOutputMeta m_currentMeta;
  private final SSTableOutputMeta m_originalMeta;

  /** various UI bits and pieces for the dialog */
  private Label m_stepnameLabel;
  private Text m_stepnameText;

  private Label m_yamlLab;
  private Button m_yamlBut;
  private TextVar m_yamlText;

  private Label m_directoryLab;
  private Button m_directoryBut;
  private TextVar m_directoryText;

  private Label m_keyspaceLab;
  private TextVar m_keyspaceText;

  private Label m_tableLab;
  private TextVar m_tableText;

  private Label m_keyFieldLab;
  private CCombo m_keyFieldCombo;

  private Label m_bufferSizeLab;
  private TextVar m_bufferSizeText;

  private Button m_getFieldsBut;

  public SSTableOutputDialog( Shell parent, Object in, TransMeta tr, String name ) {

    super( parent, (BaseStepMeta) in, tr, name );

    m_currentMeta = (SSTableOutputMeta) in;
    m_originalMeta = (SSTableOutputMeta) m_currentMeta.clone();
  }

  public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );

    props.setLook( shell );
    setShellImage( shell, m_currentMeta );

    // used to listen to a text field (m_wStepname)
    final ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
      }
    };

    changed = m_currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    m_stepnameLabel = new Label( shell, SWT.RIGHT );
    m_stepnameLabel.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.StepName.Label" ) );
    props.setLook( m_stepnameLabel );

    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    fd.top = new FormAttachment( 0, margin );
    m_stepnameLabel.setLayoutData( fd );
    m_stepnameText = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    m_stepnameText.setText( stepname );
    props.setLook( m_stepnameText );
    m_stepnameText.addModifyListener( lsMod );

    // format the text field
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_stepnameText.setLayoutData( fd );

    // yaml file line
    m_yamlLab = new Label( shell, SWT.RIGHT );
    props.setLook( m_yamlLab );
    m_yamlLab.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.YAML.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_stepnameText, margin );
    fd.right = new FormAttachment( middle, -margin );
    m_yamlLab.setLayoutData( fd );

    m_yamlBut = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( m_yamlBut );
    m_yamlBut.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.YAML.Button" ) );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_stepnameText, margin );
    m_yamlBut.setLayoutData( fd );

    m_yamlText = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_yamlText );
    m_yamlText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_yamlText.setToolTipText( transMeta.environmentSubstitute( m_yamlText.getText() ) );
      }
    } );
    m_yamlText.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( m_yamlBut, 0 );
    fd.top = new FormAttachment( m_stepnameText, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_yamlText.setLayoutData( fd );

    m_yamlBut.addSelectionListener(  new SelectionAdapterFileDialogTextVar( log, m_yamlText, transMeta,
      new SelectionAdapterOptions( SelectionOperation.FILE,
        new FilterType[] { FilterType.YAML, FilterType.ALL }, FilterType.YAML ) ) );

    // directory line
    m_directoryLab = new Label( shell, SWT.RIGHT );
    props.setLook( m_directoryLab );
    m_directoryLab.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.Directory.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_yamlText, margin );
    fd.right = new FormAttachment( middle, -margin );
    m_directoryLab.setLayoutData( fd );

    m_directoryBut = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( m_directoryBut );
    m_directoryBut.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.Directory.Button" ) );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_yamlText, margin );
    m_directoryBut.setLayoutData( fd );

    m_directoryText = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_directoryText );
    m_directoryText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_directoryText.setToolTipText( transMeta.environmentSubstitute( m_directoryText.getText() ) );
      }
    } );
    m_directoryText.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( m_directoryBut, 0 );
    fd.top = new FormAttachment( m_yamlText, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_directoryText.setLayoutData( fd );

    m_directoryBut.addSelectionListener( new SelectionAdapterFileDialogTextVar( log, m_directoryText, transMeta,
      new SelectionAdapterOptions( SelectionOperation.FOLDER ) ) );

    // keyspace line
    m_keyspaceLab = new Label( shell, SWT.RIGHT );
    props.setLook( m_keyspaceLab );
    m_keyspaceLab.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.Keyspace.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_directoryText, margin );
    fd.right = new FormAttachment( middle, -margin );
    m_keyspaceLab.setLayoutData( fd );

    m_keyspaceText = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_keyspaceText );
    m_keyspaceText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_keyspaceText.setToolTipText( transMeta.environmentSubstitute( m_keyspaceText.getText() ) );
      }
    } );
    m_keyspaceText.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_directoryText, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_keyspaceText.setLayoutData( fd );

    // table line
    m_tableLab = new Label( shell, SWT.RIGHT );
    props.setLook( m_tableLab );
    m_tableLab.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.Table.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_keyspaceText, margin );
    fd.right = new FormAttachment( middle, -margin );
    m_tableLab.setLayoutData( fd );

    m_tableText = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_tableText );
    m_tableText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_tableText.setToolTipText( transMeta.environmentSubstitute( m_tableText.getText() ) );
      }
    } );
    m_tableText.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_keyspaceText, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_tableText.setLayoutData( fd );

    // key field line
    m_keyFieldLab = new Label( shell, SWT.RIGHT );
    props.setLook( m_keyFieldLab );
    m_keyFieldLab.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.KeyField.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_tableText, margin );
    fd.right = new FormAttachment( middle, -margin );
    m_keyFieldLab.setLayoutData( fd );

    m_getFieldsBut = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( m_getFieldsBut );
    m_getFieldsBut.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.GetFields.Button" ) );

    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_tableText, 0 );
    m_getFieldsBut.setLayoutData( fd );

    m_getFieldsBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        showEnterSelectionDialog();
      }
    } );


    m_keyFieldCombo = new CCombo( shell, SWT.BORDER );
    m_keyFieldCombo.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_keyFieldCombo.setToolTipText( transMeta.environmentSubstitute( m_keyFieldCombo.getText() ) );
      }
    } );
    m_keyFieldCombo.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( m_getFieldsBut, -margin );
    fd.top = new FormAttachment( m_tableText, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_keyFieldCombo.setLayoutData( fd );

    // buffer size
    m_bufferSizeLab = new Label( shell, SWT.RIGHT );
    props.setLook( m_bufferSizeLab );
    m_bufferSizeLab.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.BufferSize.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_keyFieldCombo, margin );
    fd.right = new FormAttachment( middle, -margin );
    m_bufferSizeLab.setLayoutData( fd );

    m_bufferSizeText = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_bufferSizeText );
    m_bufferSizeText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_bufferSizeText.setToolTipText( transMeta.environmentSubstitute( m_bufferSizeText.getText() ) );
      }
    } );
    m_bufferSizeText.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_keyFieldCombo, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_bufferSizeText.setLayoutData( fd );

    // Buttons inherited from BaseStepDialog
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, m_bufferSizeText );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    m_stepnameText.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    setSize();

    getData();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return stepname;
  }


  protected void onCql3CheckSelection() {
    m_getFieldsBut.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.SelectFields.Button" ) ); //$NON-NLS-1$
    m_keyFieldLab.setText( BaseMessages.getString( PKG, "SSTableOutputDialog.KeyFields.Label" ) ); //$NON-NLS-1$
  }

  protected void setupFieldsCombo() {
    // try and set up from incoming fields from previous step

    StepMeta stepMeta = transMeta.findStep( stepname );

    if ( stepMeta != null ) {
      try {
        RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

        if ( row.size() == 0 ) {
          MessageDialog.openError( shell, BaseMessages.getString( PKG,
              "SSTableOutputData.Message.NoIncomingFields.Title" ), BaseMessages.getString( PKG,
              "SSTableOutputData.Message.NoIncomingFields" ) );

          return;
        }

        m_keyFieldCombo.removeAll();
        for ( int i = 0; i < row.size(); i++ ) {
          ValueMetaInterface vm = row.getValueMeta( i );
          m_keyFieldCombo.add( vm.getName() );
        }
      } catch ( KettleException ex ) {
        MessageDialog.openError( shell, BaseMessages
            .getString( PKG, "SSTableOutputData.Message.NoIncomingFields.Title" ), BaseMessages.getString( PKG,
            "SSTableOutputData.Message.NoIncomingFields" ) );
      }
    }
  }

  protected void ok() {
    if ( Utils.isEmpty( m_stepnameText.getText() ) ) {
      return;
    }

    stepname = m_stepnameText.getText();
    m_currentMeta.setYamlPath( m_yamlText.getText() );
    m_currentMeta.setDirectory( m_directoryText.getText() );
    m_currentMeta.setCassandraKeyspace( m_keyspaceText.getText() );
    m_currentMeta.setTableName( m_tableText.getText() );
    m_currentMeta.setKeyField( m_keyFieldCombo.getText() );
    m_currentMeta.setBufferSize( m_bufferSizeText.getText() );

    if ( !m_originalMeta.equals( m_currentMeta ) ) {
      m_currentMeta.setChanged();
      changed = m_currentMeta.hasChanged();
    }

    dispose();
  }

  protected void cancel() {
    stepname = null;
    m_currentMeta.setChanged( changed );

    dispose();
  }

  protected void getData() {

    if ( !Utils.isEmpty( m_currentMeta.getYamlPath() ) ) {
      m_yamlText.setText( m_currentMeta.getYamlPath() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getDirectory() ) ) {
      m_directoryText.setText( m_currentMeta.getDirectory() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getCassandraKeyspace() ) ) {
      m_keyspaceText.setText( m_currentMeta.getCassandraKeyspace() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getTableName() ) ) {
      m_tableText.setText( m_currentMeta.getTableName() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getKeyField() ) ) {
      m_keyFieldCombo.setText( m_currentMeta.getKeyField() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getBufferSize() ) ) {
      m_bufferSizeText.setText( m_currentMeta.getBufferSize() );
    }

    onCql3CheckSelection();
  }

  protected void showEnterSelectionDialog() {
    StepMeta stepMeta = transMeta.findStep( stepname );

    String[] choices = null;
    if ( stepMeta != null ) {
      try {
        RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

        if ( row.size() == 0 ) {
          MessageDialog.openError( shell, BaseMessages.getString( PKG,
              "SSTableOutputData.Message.NoIncomingFields.Title" ), //$NON-NLS-1$
              BaseMessages.getString( PKG, "SSTableOutputData.Message.NoIncomingFields" ) ); //$NON-NLS-1$

          return;
        }

        choices = new String[row.size()];
        for ( int i = 0; i < row.size(); i++ ) {
          ValueMetaInterface vm = row.getValueMeta( i );
          choices[i] = vm.getName();
        }

        EnterSelectionDialog dialog =
            new EnterSelectionDialog( shell, choices, BaseMessages.getString( PKG,
                "CassandraOutputDialog.SelectKeyFieldsDialog.Title" ), //$NON-NLS-1$
                BaseMessages.getString( PKG, "CassandraOutputDialog.SelectKeyFieldsDialog.Message" ), 370, 280 ); //$NON-NLS-1$
        dialog.setMulti( true );
        if ( !Utils.isEmpty( m_keyFieldCombo.getText() ) ) {
          String current = m_keyFieldCombo.getText();
          String[] parts = current.split( "," ); //$NON-NLS-1$
          int[] currentSelection = new int[parts.length];
          int count = 0;
          for ( String s : parts ) {
            int index = row.indexOfValue( s.trim() );
            if ( index >= 0 ) {
              currentSelection[count++] = index;
            }
          }

          dialog.setSelectedNrs( currentSelection );
        }

        dialog.open();

        int[] selected = dialog.getSelectionIndeces(); // SIC
        if ( selected != null && selected.length > 0 ) {
          StringBuilder newSelection = new StringBuilder();
          boolean first = true;
          for ( int i : selected ) {
            if ( first ) {
              newSelection.append( choices[i] );
              first = false;
            } else {
              newSelection.append( "," ).append( choices[i] ); //$NON-NLS-1$
            }
          }

          m_keyFieldCombo.setText( newSelection.toString() );
        }
      } catch ( KettleException ex ) {
        MessageDialog.openError( shell, BaseMessages.getString( PKG,
            "CassandraOutputData.Message.NoIncomingFields.Title" ), BaseMessages //$NON-NLS-1$
            .getString( PKG, "CassandraOutputData.Message.NoIncomingFields" ) ); //$NON-NLS-1$
      }
    }
  }

}
