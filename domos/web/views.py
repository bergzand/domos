from flask import *
class views:

    def __init__(self,app):
        self.app=app
        self.app.add_url_rule('/','main',self.main,methods=['GET'])
        self.app.add_url_rule('/content/homepage','homepage',self.homepage,methods=['GET'])
        self.app.add_url_rule('/content/modules','modules',self.modules,methods=['GET'])
        self.app.add_url_rule('/content/module','module',self.module,methods=['GET'])
        self.app.add_url_rule('/content/dashi','dashi',self.dashi,methods=['GET'])

    def main(self):
        return render_template('main.html')
    
    #@auth.login_required
    def homepage(self):
        return render_template('content/homepage.html')
    def modules(self):
        return render_template('content/modules.html')
    def module(self):
        return render_template('content/module.html')
    def dashi(self):
        return render_template('content/dashi.html')

