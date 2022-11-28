import re
def get_cleaned_text(text):
    # if text is None or len(str(text)) == 1:
    #     return ''
    stopwords = ['a', 'the', 'of', 'on', 'in', 'an', 'and', 'is', 'at', 'are', 'as', 'be', 'but', 'by', 'for', 'it',
                 'no', 'not', 'or', 'such', 'that', 'their', 'there', 'these', 'to', 'was', 'with', 'they', 'will',
                 'v', 've', 'd']

    cleaned = re.sub('[\W_]+', ' ', str(text).encode('ascii', 'ignore').decode('ascii')).lower()
    feature_one = re.sub(' +', ' ', cleaned).strip()
    punct = [',', '.', '!', ';', ':', '?', "'", '"', '\t', '\n']

    for x in stopwords:
        feature_one = feature_one.replace(' {} '.format(x), ' ')
        if feature_one.startswith('{} '.format(x)):
            feature_one = feature_one[len('{} '.format(x)):]
        if feature_one.endswith(' {}'.format(x)):
            feature_one = feature_one[:-len(' {}'.format(x))]

    for x in punct:
        feature_one = feature_one.replace('{}'.format(x), ' ')
    return feature_one