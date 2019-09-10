# -*- coding: utf-8 -*-1
__author__ = 'Administrator'

import pytesseract
from PIL import Image,ImageEnhance,ImageFilter

def cleanBlack(im):
    width = im.size[0]
    height = im.size[1]
    for i in range(0, width):#遍历所有长度的点
        for j in range(0, height):#遍历所有宽度的点
            data = (im.getpixel((i,j)))#打印该图片的所有点
            if (data[0] + data[1] + data[2])/3 < 30: #替换黑色
                 im.putpixel((i, j), (255, 255, 255))
    return im

def verify_code(im):

    #去边
    bbox = im.getbbox()
    new_bbox = (bbox[0] + 20, bbox[1] + 1, bbox[2] - 3, bbox[3] - 2)
    im = im.crop(new_bbox)

    #去除黑色
    im = cleanBlack(im)

    #灰度
    im = im.convert('L')

    #降噪
    threshold = 230
    table = []
    for i in range(256):
        if i < threshold:
            table.append(0)
        else:
            table.append(1)
    im = im.point(table, '1')
    return pytesseract.image_to_string(im, 'eng')

